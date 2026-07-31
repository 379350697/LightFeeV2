"""Credential-free public market data client for V2 sidecar.

MarketDataClient provides public HTTP access to exchange funding, ticker,
order-book, and liquidity data. It never requires LiveCredential and never
touches order/position/account endpoints.

VenueTransport inherits from MarketDataClient for its public-data needs
while adding private trading methods (order, position, account risk).
"""

from __future__ import annotations

import asyncio
import contextvars
import math
import random
import time
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Optional

import httpx

from lightfee.core.domain import Venue
from lightfee.marketdata.open_interest import (
    OpenInterestEvidence,
    OpenInterestEvidenceStatus,
    normalize_open_interest_status,
    open_interest_sample_id,
    validated_open_interest_evidence,
)
from lightfee.sidecar.snapshot import funding_rate_sample_id
from lightfee.venues.hyperliquid_info_coordinator import (
    hyperliquid_info_coordinator,
    should_coordinate_hyperliquid_info_request,
)
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
    # Local arrival of the BBO payload. Later funding/metadata/OI requests
    # must never refresh this market-data clock.
    market_received_at_ms: int = 0
    market_event_at_ms: int = 0
    bid_size: float = 0.0
    ask_size: float = 0.0
    mark_price: float = 0.0
    index_price: float = 0.0
    funding_rate_bps: float = 0.0
    # Independent funding-alpha clock.  A later BBO response must never make
    # an older funding observation look fresh.
    funding_rate_observed_at_ms: int = 0
    funding_rate_event_at_ms: int = 0
    funding_rate_received_at_ms: int = 0
    funding_rate_source: str = ""
    funding_rate_sample_id: str = ""
    funding_timestamp_ms: int = 0
    # The public source may expose a next/predicted rate separately from the
    # last settled/current rate.  ``None`` deliberately means unavailable;
    # callers must not silently turn that into a high-confidence forecast.
    predicted_funding_rate_bps: float | None = None
    funding_forecast_source: str = "quoted_rate"
    funding_forecast_sample_count: int = 0
    # Confirmed, already-settled rate used only for forecast calibration.
    settled_funding_rate_bps: float | None = None
    # The interval is only populated from exchange evidence (or a measured
    # transition of the exchange's next-settlement timestamp).  Never assume
    # a universal eight-hour interval here.
    funding_interval_ms: int = 0
    funding_interval_source: str = ""
    funding_interval_observed_at_ms: int = 0
    volume_24h_quote: float = 0.0
    open_interest_quote: float | None = None
    open_interest_evidence_status: str = "unavailable"
    open_interest_evidence_reason: str = ""
    open_interest_observed_at_ms: int = 0
    open_interest_event_at_ms: int = 0
    open_interest_received_at_ms: int = 0
    open_interest_source: str = ""
    open_interest_sample_id: str = ""
    open_interest_venue_symbol: str = ""
    raw_open_interest: float | None = None
    raw_open_interest_unit: str = ""
    open_interest_contract_multiplier: float | None = None
    open_interest_conversion_mark_price: float | None = None
    oi_candidate_count: int = 0
    oi_cache_hit_count: int = 0
    oi_cache_miss_count: int = 0
    oi_refresh_attempt_count: int = 0
    oi_refresh_cap: int = 0
    oi_deferred_count: int = 0
    oi_timeout_count: int = 0
    oi_refresh_elapsed_ms: int = 0
    oi_dns_ms: int | None = None
    oi_connect_ms: int = 0
    oi_pool_wait_ms: int = 0
    oi_rate_limit_wait_ms: int = 0
    oi_transport_total_ms: int = 0
    oi_http_ms: int = 0
    oi_parse_ms: int = 0
    oi_dns_timing_status: str = "unavailable"
    # Contract-normalisation evidence consumed by the spread strategy.  The
    # quantities emitted by this public client are already base quantities;
    # multiplier therefore describes the normalised economic unit.
    underlying: str = ""
    quote_currency: str = ""
    contract_type: str = ""
    contract_multiplier: float = 0.0
    mark_index_source: str = ""
    price_precision: int = 0
    quantity_precision: int = 0
    venue_status: str = "unknown"
    contract_normalization_complete: bool = False
    # Parser-owned proof that BBO sizes are already canonical base quantity.
    # ``price_tick`` and ``quantity_step_base`` must come from symbol-level
    # exchange metadata when a venue quotes derivatives in contract lots.
    base_quantity_evidence: bool = False
    price_tick: float = 0.0
    quantity_step_base: float = 0.0
    min_quantity_base: float = 0.0
    min_notional_quote: float = 0.0
    min_notional_evidence_complete: bool = False


_PUBLIC_REQUEST_PHASE_COLLECTOR: contextvars.ContextVar[
    list[dict[str, Any]] | None
] = contextvars.ContextVar("lightfee_public_request_phase_collector", default=None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, str) and value.strip() == "":
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _has_nonempty_field(item: dict[str, Any], *keys: str) -> bool:
    for key in keys:
        if key not in item:
            continue
        value = item.get(key)
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return True
    return False


def _funding_rate_fields(
    *,
    venue: str,
    symbol: str,
    rate_bps: float,
    funding_timestamp_ms: int,
    observed_at_ms: int,
    source: str,
    event_at_ms: int = 0,
) -> dict[str, Any]:
    """Build one immutable funding-alpha receipt without inventing time."""
    observed = int(observed_at_ms or 0)
    event = int(event_at_ms or 0)
    if observed <= 0:
        return {
            "funding_rate_observed_at_ms": 0,
            "funding_rate_event_at_ms": 0,
            "funding_rate_received_at_ms": 0,
            "funding_rate_source": "",
            "funding_rate_sample_id": "",
        }
    if event < 0 or event > observed + 5_000:
        event = 0
    sample_id = funding_rate_sample_id(
        venue=venue,
        symbol=symbol,
        observed_at_ms=observed,
        rate_bps=rate_bps,
        funding_timestamp_ms=funding_timestamp_ms,
    )
    return {
        "funding_rate_observed_at_ms": observed,
        "funding_rate_event_at_ms": event,
        "funding_rate_received_at_ms": observed,
        "funding_rate_source": str(source or ""),
        "funding_rate_sample_id": sample_id,
    }


def _positive_exchange_number(value: Any) -> float:
    """Parse an exchange JSON number/string while rejecting JSON booleans."""
    if isinstance(value, bool):
        return 0.0
    parsed = _safe_float(value)
    return parsed if math.isfinite(parsed) and parsed > 0.0 else 0.0


def _gate_contract_metadata_complete(item: dict[str, Any]) -> bool:
    """One predicate for both Gate retry/liveness and unit admission."""
    return bool(
        _positive_exchange_number(item.get("quanto_multiplier")) > 0.0
        and _positive_exchange_number(item.get("order_size_min")) > 0.0
        and _positive_exchange_number(item.get("order_price_round")) > 0.0
        and item.get("in_delisting") is False
    )


def _binance_style_symbol_increments(
    item: dict[str, Any],
) -> tuple[float, float]:
    filters = item.get("filters", [])
    if not isinstance(filters, list):
        return 0.0, 0.0
    price_tick = 0.0
    quantity_step = 0.0
    for raw_filter in filters:
        if not isinstance(raw_filter, dict):
            continue
        filter_type = str(raw_filter.get("filterType", "") or "").upper()
        if filter_type == "PRICE_FILTER":
            price_tick = _positive_exchange_number(raw_filter.get("tickSize"))
        elif filter_type == "LOT_SIZE":
            quantity_step = _positive_exchange_number(raw_filter.get("stepSize"))
    return price_tick, quantity_step


def _binance_style_symbol_minimums(
    item: dict[str, Any],
) -> tuple[float, float]:
    filters = item.get("filters", [])
    if not isinstance(filters, list):
        return 0.0, 0.0
    min_quantity = 0.0
    exchange_min_notional = 0.0
    for raw_filter in filters:
        if not isinstance(raw_filter, dict):
            continue
        filter_type = str(raw_filter.get("filterType", "") or "").upper()
        if filter_type == "LOT_SIZE":
            min_quantity = _positive_exchange_number(raw_filter.get("minQty"))
        elif filter_type in {"MIN_NOTIONAL", "NOTIONAL"}:
            exchange_min_notional = _positive_exchange_number(
                raw_filter.get("notional", raw_filter.get("minNotional"))
            )
    return min_quantity, exchange_min_notional


def _binance_style_contract_metadata_complete(
    item: dict[str, Any],
    *,
    underlying: str,
    quote_currency: str,
) -> bool:
    price_tick, quantity_step = _binance_style_symbol_increments(item)
    min_quantity, _ = _binance_style_symbol_minimums(item)
    return bool(
        str(item.get("status", "") or "").upper() == "TRADING"
        and str(item.get("contractType", "") or "").upper() == "PERPETUAL"
        and str(item.get("baseAsset", "") or "").upper() == underlying
        and str(item.get("quoteAsset", "") or "").upper() == quote_currency
        and str(item.get("marginAsset", quote_currency) or "").upper()
        == quote_currency
        and price_tick > 0.0
        and quantity_step > 0.0
        and min_quantity > 0.0
    )


def _hyperliquid_size_decimals(item: dict[str, Any]) -> int | None:
    value = item.get("szDecimals")
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    if value < 0 or value > 6:
        return None
    delisted = item.get("isDelisted")
    # Hyperliquid omits this optional flag for active instruments.  If it is
    # present, only a literal JSON false is admissible as active evidence.
    if delisted is not None and delisted is not False:
        return None
    return value


def _hyperliquid_price_tick(reference_price: float, size_decimals: int) -> float:
    """Return the current valid perp price quantum under Hyperliquid rules."""
    if not math.isfinite(reference_price) or reference_price <= 0.0:
        return 0.0
    max_decimals = 6 - size_decimals
    magnitude = math.floor(math.log10(reference_price))
    significant_decimals = max(4 - magnitude, 0)
    decimals = min(max_decimals, significant_decimals)
    return 10.0 ** (-decimals)


def _first_present_float(
    item: dict[str, Any],
    *keys: str,
    default: float = 0.0,
) -> tuple[float, bool, str]:
    for key in keys:
        if _has_nonempty_field(item, key):
            value = item.get(key)
            if isinstance(value, bool):
                return default, False, key
            try:
                parsed = float(value)
            except (TypeError, ValueError, OverflowError):
                return default, False, key
            if not math.isfinite(parsed) or parsed < 0.0:
                return default, False, key
            return parsed, True, key
    return default, False, ""


def _bybit_total_open_interest(
    item: dict[str, Any],
) -> tuple[float | None, float | None, str, str, str]:
    """Return Bybit OI on the configured total-open-interest basis.

    ``singleOpenInterestValue`` is deliberately not a fallback for
    ``openInterestValue``.  It is a different economic measure and Bybit does
    not publish a contract guaranteeing a lossless conversion to total OI.
    """
    total_value, total_valid, total_key = _first_present_float(
        item,
        "openInterestValue",
    )
    if total_valid:
        return total_value, total_value, "quote", "observed", total_key
    if total_key:
        return None, None, "", "parse_error", f"invalid_{total_key}"

    single_value, single_valid, single_key = _first_present_float(
        item,
        "singleOpenInterestValue",
    )
    if single_valid:
        return (
            None,
            single_value,
            "single_side_quote",
            "unsupported",
            "singleOpenInterestValue_is_not_total_open_interest",
        )
    if single_key:
        return None, None, "", "parse_error", f"invalid_{single_key}"
    return None, None, "", "parse_error", "missing_open_interest_value"


_ENTRY_OI_RATE_LIMIT_CODES: dict[Venue, frozenset[str]] = {
    Venue.OKX: frozenset({"429", "50011", "50040"}),
    Venue.BYBIT: frozenset({"429", "10006", "10429"}),
    Venue.BITGET: frozenset({"429", "40010", "40725"}),
    Venue.GATE: frozenset({"429", "TOO_BUSY", "TOO_MANY_REQUESTS"}),
    Venue.HYPERLIQUID: frozenset({"429", "TOO_MANY_REQUESTS"}),
}


def _entry_oi_application_error(
    venue: Venue,
    payload: Any,
) -> tuple[str, str] | None:
    """Classify HTTP-200 exchange errors without inventing mapping failures."""
    code: object | None = None
    message: object | None = None

    if venue == Venue.OKX and isinstance(payload, dict) and "code" in payload:
        code = payload.get("code")
        if str(code or "").strip() in {"", "0"}:
            return None
        message = payload.get("msg")
    elif venue == Venue.BYBIT and isinstance(payload, dict) and "retCode" in payload:
        code = payload.get("retCode")
        if str(code or "").strip() in {"", "0"}:
            return None
        message = payload.get("retMsg")
    elif venue == Venue.BITGET and isinstance(payload, dict) and "code" in payload:
        code = payload.get("code")
        if str(code or "").strip() in {"", "0", "00000"}:
            return None
        message = payload.get("msg") or payload.get("message")
    elif venue == Venue.GATE and isinstance(payload, dict) and (
        "label" in payload
        or "error" in payload
        or ("code" in payload and "message" in payload)
        or str(payload.get("status") or "").strip().lower()
        in {"error", "failed", "fail"}
    ):
        code = payload.get("label") or payload.get("code") or payload.get("status")
        message = payload.get("message") or payload.get("error")
    elif venue == Venue.HYPERLIQUID and isinstance(payload, dict) and (
        "error" in payload
        or str(payload.get("status") or "").strip().lower()
        in {"error", "err", "failed", "fail"}
    ):
        code = payload.get("code") or payload.get("status") or "error"
        message = payload.get("error") or payload.get("response") or payload.get("message")
    else:
        return None

    code_s = str(code or "unknown").strip()
    message_s = str(message or "application_error").strip()
    normalized = f"{code_s} {message_s}".lower()
    rate_limited = (
        code_s.upper() in _ENTRY_OI_RATE_LIMIT_CODES.get(venue, frozenset())
        or any(
            hint in normalized
            for hint in (
                "rate limit",
                "rate-limit",
                "too many",
                "too frequent",
                "frequency limit",
                "limit reached",
            )
        )
    )
    # OKX returns HTTP 200 with code 51001 when an instId does not exist.
    # Treat that as a catalog absence so an unlisted alias cannot masquerade
    # as a transport fault or poison otherwise healthy venue coverage.
    if venue == Venue.OKX and code_s == "51001":
        status = "symbol_not_listed"
    else:
        status = "rate_limited" if rate_limited else "http_error"
    reason = f"{venue.value}_application_error:{code_s}:{message_s}"[:200]
    return status, reason


def _open_interest_fields(
    *,
    venue: str,
    canonical_symbol: str,
    venue_symbol: str,
    value_quote: float | None,
    raw_value: float | None,
    raw_unit: str,
    contract_multiplier: float | None,
    conversion_mark_price: float | None,
    observed_at_ms: int,
    received_at_ms: int,
    source: str,
    status: str,
    reason: str,
) -> dict[str, object]:
    normalized_status = normalize_open_interest_status(status)
    # Adapter callers pass the native exchange timestamp as ``observed_at_ms``
    # for backward compatibility.  The evidence contract stores that on the
    # exchange clock and records local arrival separately.
    event_at_ms = max(int(observed_at_ms or 0), 0)
    local_observed_at_ms = (
        int(received_at_ms)
        if normalized_status == OpenInterestEvidenceStatus.OBSERVED.value
        else 0
    )
    sample_id = ""
    if normalized_status == OpenInterestEvidenceStatus.OBSERVED.value:
        sample_id = open_interest_sample_id(
            venue=venue,
            canonical_symbol=canonical_symbol,
            venue_symbol=venue_symbol,
            observed_at_ms=event_at_ms or local_observed_at_ms,
            source=source,
            raw_value=raw_value,
            value_quote=value_quote,
        )
    evidence = validated_open_interest_evidence(
        OpenInterestEvidence(
            canonical_symbol=canonical_symbol,
            venue_symbol=venue_symbol,
            value_quote=value_quote,
            raw_value=raw_value,
            raw_unit=raw_unit,
            contract_multiplier=contract_multiplier,
            conversion_mark_price=conversion_mark_price,
            observed_at_ms=local_observed_at_ms or None,
            event_at_ms=event_at_ms or None,
            received_at_ms=received_at_ms,
            source=source,
            status=normalized_status,
            reason=reason,
            sample_id=sample_id,
        )
    )
    return {
        "open_interest_quote": evidence.value_quote,
        "open_interest_evidence_status": evidence.status,
        "open_interest_evidence_reason": evidence.reason,
        "open_interest_observed_at_ms": int(evidence.observed_at_ms or 0),
        "open_interest_event_at_ms": int(evidence.event_at_ms or 0),
        "open_interest_received_at_ms": int(evidence.received_at_ms or 0),
        "open_interest_source": evidence.source,
        "open_interest_sample_id": evidence.sample_id,
        "open_interest_venue_symbol": evidence.venue_symbol,
        "raw_open_interest": evidence.raw_value,
        "raw_open_interest_unit": evidence.raw_unit,
        "open_interest_contract_multiplier": evidence.contract_multiplier,
        "open_interest_conversion_mark_price": evidence.conversion_mark_price,
    }


def _entry_oi_observed_at_ms(
    native_timestamp: object,
    *,
    received_at_ms: int,
    field_name: str,
) -> tuple[int, str | None, str | None]:
    """Parse the independent exchange event timestamp.

    An absent vendor timestamp is valid and returns zero: local receipt time is
    still the freshness boundary.  Small positive vendor-clock skew is kept as
    evidence rather than compared as if both clocks were identical.
    """
    if native_timestamp is None or (
        isinstance(native_timestamp, str) and not native_timestamp.strip()
    ):
        return 0, None, None
    if isinstance(native_timestamp, bool):
        return 0, "parse_error", f"invalid_{field_name}"
    try:
        parsed = Decimal(str(native_timestamp).strip())
    except (InvalidOperation, ValueError):
        return 0, "parse_error", f"invalid_{field_name}"
    if (
        not parsed.is_finite()
        or parsed <= 0
        or parsed != parsed.to_integral_value()
    ):
        return 0, "parse_error", f"invalid_{field_name}"
    observed_at_ms = int(parsed)
    if observed_at_ms > int(received_at_ms) + 5_000:
        return 0, "stale", f"future_{field_name}"
    return observed_at_ms, None, None


def _http_error_reason(exc: Exception) -> str:
    message = str(exc).strip()
    return message[:200] if message else type(exc).__name__


def _now_ms() -> int:
    return int(time.time() * 1000)


def _optional_rate_bps(item: dict[str, Any], *keys: str) -> float | None:
    """Return a vendor-supplied rate in bps without inventing a forecast."""
    for key in keys:
        if _has_nonempty_field(item, key):
            return _safe_float(item.get(key)) * 10_000.0
    return None


def _decimal_precision(step: float) -> int:
    """Return decimal places implied by a configured exchange increment."""
    if not math.isfinite(step) or step <= 0.0:
        return 0
    try:
        normalized = Decimal(str(step)).normalize()
    except (InvalidOperation, ValueError):
        return 0
    if normalized <= 0:
        return 0
    return max(-normalized.as_tuple().exponent, 0)


def _canonical_contract_identity(venue: Venue, symbol: str) -> tuple[str, str]:
    """Derive the canonical underlying/quote only for known perp endpoints."""
    canonical = str(symbol or "").upper().replace("-", "").replace("_", "")
    for quote in ("USDT", "USDC", "USD"):
        if canonical.endswith(quote) and len(canonical) > len(quote):
            return canonical[: -len(quote)], quote
    # Hyperliquid's public perp universe is coin-denominated and USDC settled.
    if venue == Venue.HYPERLIQUID and canonical:
        return canonical, "USDC"
    return "", ""


# V1 parity: Binance-compatible OI is a per-symbol endpoint, fetched with bounded
# concurrency and normalized to quote notional via premiumIndex mark price.
_BINANCE_STYLE_OPEN_INTEREST_CONCURRENCY = 16
BINANCE_STYLE_OPEN_INTEREST_ENRICHMENT_BUDGET_S = 0.1
BINANCE_STYLE_ENTRY_OPEN_INTEREST_BUDGET_S = 2.0
BINANCE_STYLE_OPEN_INTEREST_CACHE_MAX_AGE_MS = 10 * 60 * 1_000
BINANCE_STYLE_OPEN_INTEREST_REFRESH_CAP = 128
# V1 parity: per-symbol OKX funding-rate concurrency limit.  The batch endpoint
# returns the full swap universe and is the production fast path.  Its timeout
# must cover a realistic full payload; the bounded fallback prevents a failed
# batch from turning one refresh into hundreds of requests.
_OKX_FUNDING_RATE_SEMAPHORE = 40
_OKX_FUNDING_RATE_PER_SYMBOL_TIMEOUT_S = 6.0
_OKX_FUNDING_RATE_BATCH_TIMEOUT_S = 1.0
_OKX_FUNDING_RATE_FALLBACK_TOTAL_TIMEOUT_S = 0.5
_OKX_FUNDING_RATE_FALLBACK_MAX_SYMBOLS = 64
# OKX market/tickers is one venue-wide response (roughly 125 KiB in
# production), not a per-symbol fan-out.  Bound each attempt separately so a
# poisoned/stale keep-alive connection cannot consume the whole sidecar domain
# budget.  One transport recycle is safe here because the funding sidecar owns
# this MarketDataClient and this request precedes its enrichment requests.
_OKX_MARKET_TICKERS_ATTEMPT_TIMEOUT_S = 3.0
_FUNDING_INTERVAL_HISTORY_SEMAPHORE = 8
_FUNDING_INTERVAL_HISTORY_BUDGET_S = 3.0
# Funding cadence is slow contract evidence, not a per-scan input.  On a cold
# cache we still have to ask the venue for symbols without an explicit
# interval, but doing that for every listed contract on every three-second
# scan self-inflicts the very rate-limit/timeout condition it is meant to
# diagnose.  A bounded, rotating batch eventually fills the persistent cache;
# normal scans then make no history requests at all.
_FUNDING_INTERVAL_HISTORY_REFRESH_CAP = 48
_FUNDING_INTERVAL_HISTORY_REFRESH_INTERVAL_MS = 30_000

# V1 parity: OKX funding cache TTL (10 min) — src/live/okx.rs OKX_FUNDING_CACHE_MAX_OBSERVED_AGE_MS
_FUNDING_CACHE_MAX_OBSERVED_AGE_MS = 10 * 60 * 1_000  # 10 minutes
# V1 parity: funding timestamp must be at least this far in the future to be cache-usable.
# When funding just settled, the exchange publishes the next funding time, so stale-on-settlement
# avoids using a just-expired timestamp.
_FUNDING_CACHE_MIN_FUTURE_MS = 30_000  # 30 seconds
_GATE_CONTRACT_METADATA_MAX_AGE_MS = 60 * 60 * 1_000
_OKX_CONTRACT_METADATA_MAX_AGE_MS = 60 * 60 * 1_000
_BINANCE_STYLE_CONTRACT_METADATA_MAX_AGE_MS = 60 * 60 * 1_000
# Contract specifications are an execution proof, but they do not change at
# BBO cadence.  Keep the raw venue-wide document locally so an unlisted symbol
# in the cross-venue universe cannot force every scan to re-download it.
_SLOW_LIQUIDITY_SNAPSHOT_MAX_AGE_MS = 60 * 1_000
_SLOW_OPEN_INTEREST_SNAPSHOT_MAX_AGE_MS = 30 * 1_000


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
        value = item.get(key, 0)
        if isinstance(value, bool):
            continue
        parsed = _safe_float(value)
        if not math.isfinite(parsed):
            continue
        ts_ms = int(parsed)
        if ts_ms > 0:
            return ts_ms
    return fallback_ms


def _funding_timestamp_ms_or_seconds(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    parsed = _safe_float(value, default=0.0)
    if not math.isfinite(parsed):
        return 0
    ts = int(parsed)
    if ts <= 0:
        return 0
    if ts < 10_000_000_000:
        return ts * 1000
    return ts


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
        http_max_connections: int | None = None,
        http_max_keepalive_connections: int = 4,
        consume_global_rate_limit_budget: bool = True,
    ) -> None:
        self._spec = spec
        self._exchange_http_timeout_ms = exchange_http_timeout_ms
        self._rate_limiter = rate_limiter
        self._http_max_connections = http_max_connections
        self._http_max_keepalive_connections = http_max_keepalive_connections
        self._consume_global_rate_limit_budget = bool(
            consume_global_rate_limit_budget
        )
        self._client: Optional[httpx.AsyncClient] = None
        # V1 parity: per-symbol funding rate cache (OKX, etc.)
        # {venue_key:symbol -> FundingTicker} with observed_at_ms
        self._funding_cache: dict[str, tuple[float, int, int]] = {}  # (rate_bps, timestamp_ms, observed_at_ms)
        self._funding_cache_observed_at_ms: int = 0
        # OKX cache provenance is a time pair, not merely the next settlement.
        # Keep it separate from the cross-venue rate cache so cache hits retain
        # the same explicit interval source and receipt time as fresh rows.
        self._okx_funding_time_pair_by_key: dict[
            str, tuple[int, int, int]
        ] = {}
        self._okx_funding_fallback_cursor: int = 0
        # Per-symbol next-settlement observations.  An interval is only
        # inferred after the exchange itself advances the next timestamp; a
        # cold process remains explicitly interval-unknown.
        self._funding_schedule_next_by_key: dict[str, int] = {}
        # key -> (interval_ms, source, evidence_observed_at_ms).  Interval
        # evidence is deliberately time-bounded because venues can temporarily
        # alter settlement cadence.
        self._funding_interval_by_key: dict[str, tuple[int, str, int]] = {}
        self._funding_interval_history_refresh_cursor = 0
        self._funding_interval_history_last_refresh_at_ms = 0
        # Gate BBO sizes are contract lots.  This symbol-level cache stays
        # separate from the shorter-lived funding cache so funding reuse can
        # never erase quantity conversion evidence.
        self._gate_contract_metadata_by_key: dict[
            str, tuple[dict[str, Any], int]
        ] = {}
        self._okx_contract_metadata_by_key: dict[
            str, tuple[dict[str, Any], int]
        ] = {}
        self._binance_style_contract_metadata_by_key: dict[
            str, tuple[dict[str, Any], int]
        ] = {}
        # Venue-wide public responses which are deliberately slower than the
        # market-data clock.  This is an in-process cache only: a failed
        # request never extends a previous response and callers retain the
        # original observation time instead of manufacturing freshness.
        self._public_payload_cache: dict[str, tuple[Any, int]] = {}
        # V1-style evidence cache for Binance-compatible per-symbol OI.
        # key -> (open_interest_quote, mark_price, observed_at_ms, status, reason)
        self._binance_style_open_interest_cache: dict[
            str, tuple[float, float, int, str, str]
        ] = {}
        self._binance_style_open_interest_inflight: dict[
            str, asyncio.Task[tuple[str, float, float, int, str, str]]
        ] = {}
        self._binance_style_open_interest_refresh_cursor: int = 0
        self._binance_style_open_interest_semaphore = asyncio.Semaphore(
            _BINANCE_STYLE_OPEN_INTEREST_CONCURRENCY
        )

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
        if self._client is None:
            timeout_s = self._exchange_http_timeout_ms / 1000.0
            limit_kwargs = {
                "max_keepalive_connections": self._http_max_keepalive_connections,
            }
            if self._http_max_connections is not None:
                limit_kwargs["max_connections"] = self._http_max_connections
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(timeout_s),
                limits=httpx.Limits(**limit_kwargs),
            )
        return self._client

    async def close(self) -> None:
        inflight = list(self._binance_style_open_interest_inflight.values())
        self._binance_style_open_interest_inflight.clear()
        for task in inflight:
            if not task.done():
                task.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _recycle_public_http_client(self) -> None:
        """Discard one client's connection pool after a transport failure."""
        client, self._client = self._client, None
        if client is None:
            return
        try:
            await client.aclose()
        except Exception:
            # The failed pool is already detached.  Cleanup failure must not
            # prevent the bounded retry from constructing a fresh transport.
            return

    async def _public_get_with_recycled_transport_retry(
        self,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        attempt_timeout_s: float,
    ) -> Any:
        """Retry one idempotent GET once on a connection-level failure.

        HTTP responses (including 4xx/5xx) are authoritative and are not
        retried here.  Only a timeout/network failure without an HTTP status
        recycles the client, keeping rate-limit and fail-closed semantics
        unchanged.
        """
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                return await asyncio.wait_for(
                    self._public_get(path, params=params),
                    timeout=max(float(attempt_timeout_s), 0.001),
                )
            except asyncio.TimeoutError:
                last_error = PublicTransportError(
                    PublicTransportErrorCategory.TRANSPORT_FAILURE,
                    f"timeout: GET {path}",
                )
            except PublicTransportError as exc:
                if int(getattr(exc, "status_code", 0) or 0) > 0:
                    raise
                last_error = exc
            if attempt == 0:
                await self._recycle_public_http_client()
        assert last_error is not None
        raise last_error

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

    async def _public_get(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        """Execute a public GET request with rate-limit pacing."""
        return await self._public_request("GET", path, params=params)

    async def _cached_public_get(
        self,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        max_age_ms: int,
    ) -> Any:
        """Return a bounded-age public GET response without faking freshness.

        Broad discovery passes the full cross-venue symbol union to every
        adapter.  A symbol not listed on one venue used to make its otherwise
        static contract endpoint run every pass.  Cache by the complete wire
        request instead of by requested symbols, so that condition cannot turn
        a negative listing lookup into repeated exchange work.
        """

        payload, _received_at_ms = await self._cached_public_get_with_received_at(
            path,
            params=params,
            max_age_ms=max_age_ms,
        )
        return payload

    async def _cached_public_get_with_received_at(
        self,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        max_age_ms: int,
    ) -> tuple[Any, int]:
        """Return a bounded-age payload together with its real receipt time."""

        ttl_ms = max(int(max_age_ms or 0), 0)
        normalized_params = tuple(
            sorted(
                (str(key), repr(value))
                for key, value in (params or {}).items()
            )
        )
        key = f"{path}?{normalized_params!r}"
        cached = self._public_payload_cache.get(key)
        if cached is not None:
            payload, observed_at_ms = cached
            now_ms = int(time.time() * 1_000)
            age_ms = now_ms - int(observed_at_ms or 0)
            if 0 <= age_ms <= ttl_ms:
                return payload, int(observed_at_ms)
            if age_ms > ttl_ms:
                self._public_payload_cache.pop(key, None)
        payload = await self._public_get(path, params=params)
        received_at_ms = int(time.time() * 1_000)
        self._public_payload_cache[key] = (payload, received_at_ms)
        return payload, received_at_ms

    async def _public_post(self, path: str, body: Optional[dict[str, Any]] = None) -> Any:
        """Execute a public POST request with rate-limit pacing."""
        return await self._public_request("POST", path, body=body)

    async def _public_get_with_received_at(
        self,
        path: str,
        params: Optional[dict[str, Any]] = None,
    ) -> tuple[Any, int]:
        # Test/replay clients commonly override ``_public_get`` to provide a
        # deterministic transport. Preserve that boundary while still stamping
        # the local arrival immediately after the await returns.
        if type(self)._public_get is not MarketDataClient._public_get:
            payload = await self._public_get(path, params=params)
            return payload, _now_ms()
        return await self._public_request_with_received_at(
            "GET", path, params=params
        )

    async def _public_post_with_received_at(
        self,
        path: str,
        body: Optional[dict[str, Any]] = None,
    ) -> tuple[Any, int]:
        if type(self)._public_post is not MarketDataClient._public_post:
            payload = await self._public_post(path, body=body)
            return payload, _now_ms()
        return await self._public_request_with_received_at(
            "POST", path, body=body
        )

    async def _public_request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        body: Optional[dict[str, Any]] = None,
    ) -> Any:
        """Send a public HTTP request with scoped rate limiting."""
        payload, _received_at_ms = await self._public_request_with_received_at(
            method, path, params=params, body=body
        )
        return payload

    async def _public_request_with_received_at(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        body: Optional[dict[str, Any]] = None,
    ) -> tuple[Any, int]:
        """Send a request and watermark it immediately after response receipt."""
        client = await self._get_client()
        base_url = self._spec.public_base_url
        scopes = self._public_rate_limit_scopes(method, path)
        phase_collector = _PUBLIC_REQUEST_PHASE_COLLECTOR.get()
        rate_limit_started = time.perf_counter()
        info_coordinator = None
        if should_coordinate_hyperliquid_info_request(
            self._spec.venue_id,
            method,
            path,
            body,
        ):
            info_coordinator = hyperliquid_info_coordinator()
            cached_info = info_coordinator.lookup_metadata_response(body)
            if cached_info is not None:
                return cached_info.payload, cached_info.received_at_ms

        if self._rate_limiter is not None:
            await self._rate_limiter.wait_until_ready_for_scopes(scopes)
            await self._rate_limiter.pace_for_scopes(scopes)

        from lightfee.rate_limit.engine import global_rate_limit_runtime as _get_global_rt
        global_rt = _get_global_rt()
        if self._consume_global_rate_limit_budget and global_rt is not None:
            await global_rt.async_wait_until_ready_for_scopes(scopes)
        if info_coordinator is not None:
            await info_coordinator.async_wait_until_ready(body)
        rate_limit_elapsed_ms = max(
            int((time.perf_counter() - rate_limit_started) * 1_000),
            0,
        )

        url = base_url + path
        transport_started = time.perf_counter()
        trace_events: dict[str, float] = {}

        async def _trace(event_name: str, _info: dict[str, Any]) -> None:
            trace_events[event_name] = time.perf_counter()

        request_kwargs: dict[str, Any] = {}
        if phase_collector is not None and isinstance(client, httpx.AsyncClient):
            request_kwargs["extensions"] = {"trace": _trace}

        phase_recorded = False
        phase_payload: dict[str, Any] = {}

        def _record_phase(*, parse_ms: int = 0) -> dict[str, Any]:
            nonlocal phase_recorded, phase_payload
            if phase_recorded:
                return dict(phase_payload)
            phase_recorded = True
            if phase_collector is None:
                return {}
            transport_finished = time.perf_counter()
            connect_started = trace_events.get("connection.connect_tcp.started")
            connect_finished = trace_events.get("connection.connect_tcp.complete")
            first_transport_event = min(trace_events.values(), default=transport_started)
            connect_ms = (
                max(int((connect_finished - connect_started) * 1_000), 0)
                if connect_started is not None and connect_finished is not None
                else 0
            )
            transport_total_ms = max(
                int((transport_finished - transport_started) * 1_000),
                0,
            )
            pool_wait_ms = max(
                int((first_transport_event - transport_started) * 1_000),
                0,
            )
            # ``http_ms`` is the residual response/request phase.  Keep the
            # total separately so operators cannot accidentally add a total
            # to its component timings and double count latency.
            http_ms = max(transport_total_ms - pool_wait_ms - connect_ms, 0)
            phase_payload = {
                    # httpcore exposes DNS+TCP as one connect_tcp phase. Keep
                    # DNS explicitly unavailable instead of fabricating a
                    # split, and retain the combined actionable duration.
                    "dns_ms": None,
                    "dns_timing_status": "included_in_connect",
                    "connect_ms": connect_ms,
                    "pool_wait_ms": pool_wait_ms,
                    "rate_limit_wait_ms": rate_limit_elapsed_ms,
                    "transport_total_ms": transport_total_ms,
                    "http_ms": http_ms,
                    "parse_ms": max(int(parse_ms), 0),
                }
            phase_collector.append(phase_payload)
            return dict(phase_payload)

        try:
            if method.upper() == "GET":
                resp = await client.get(url, params=params, **request_kwargs)
            elif method.upper() == "POST":
                resp = await client.post(url, json=body, **request_kwargs)
            else:
                raise ValueError(f"unsupported method: {method}")
            received_at_ms = _now_ms()
            if info_coordinator is not None:
                info_coordinator.record_http_response(
                    resp.status_code,
                    dict(resp.headers),
                    body=body,
                )

            if resp.status_code >= 400:
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
                phase = _record_phase()
                raise PublicTransportError(
                    PublicTransportErrorCategory.TRANSPORT_FAILURE,
                    f"HTTP {resp.status_code}: {resp.text[:200]}",
                    status_code=resp.status_code,
                    phase_timings=phase,
                )

            if self._rate_limiter is not None:
                self._rate_limiter.record_success_for_scopes(scopes)

            if not resp.text:
                _record_phase()
                return {}, received_at_ms
            parse_started = time.perf_counter()
            try:
                payload = resp.json()
            except Exception as exc:
                parse_ms = max(
                    int((time.perf_counter() - parse_started) * 1_000),
                    0,
                )
                phase = _record_phase(parse_ms=parse_ms)
                raise PublicTransportError(
                    PublicTransportErrorCategory.PARSE_FAILURE,
                    f"invalid JSON: {method} {path}: {type(exc).__name__}",
                    phase_timings=phase,
                ) from exc
            finally:
                parse_ms = max(
                    int((time.perf_counter() - parse_started) * 1_000),
                    0,
                )
                _record_phase(parse_ms=parse_ms)
            if info_coordinator is not None:
                info_coordinator.store_metadata_response(
                    body,
                    payload,
                    received_at_ms=received_at_ms,
                )
            return payload, received_at_ms
        except httpx.TimeoutException:
            phase = _record_phase()
            raise PublicTransportError(
                PublicTransportErrorCategory.TRANSPORT_FAILURE,
                f"timeout: {method} {path}",
                phase_timings=phase,
            )
        except httpx.NetworkError as e:
            phase = _record_phase()
            raise PublicTransportError(
                PublicTransportErrorCategory.TRANSPORT_FAILURE,
                f"network: {method} {path}: {e}",
                phase_timings=phase,
            )

    # ------------------------------------------------------------------
    # Funding ticker fetch — main sidecar entry point
    # ------------------------------------------------------------------

    async def fetch_funding_tickers(
        self,
        symbols: list[str],
        *,
        include_open_interest: bool = True,
    ) -> dict[str, FundingTicker]:
        """Fetch funding + bid/ask + mark + volume + OI for requested symbols.

        Returns {venue_key:symbol -> FundingTicker}.
        """
        venue_id = self._spec.venue_id

        if venue_id in (Venue.BINANCE, Venue.ASTER):
            tickers = await self._fetch_binance_style(
                symbols,
                include_open_interest=include_open_interest,
            )
        elif venue_id == Venue.OKX:
            tickers = await self._fetch_okx_style(
                symbols,
                include_open_interest=include_open_interest,
            )
        elif venue_id == Venue.BYBIT:
            tickers = await self._fetch_bybit_style(symbols)
        elif venue_id == Venue.BITGET:
            tickers = await self._fetch_bitget_style(symbols)
        elif venue_id == Venue.GATE:
            tickers = await self._fetch_gate_style(symbols)
        elif venue_id == Venue.HYPERLIQUID:
            tickers = await self._fetch_hyperliquid_style(symbols)
        else:
            tickers = {}
        return self._enrich_tickers(tickers, observed_at_ms=_now_ms())

    def _enrich_tickers(
        self,
        tickers: dict[str, FundingTicker],
        *,
        observed_at_ms: int,
    ) -> dict[str, FundingTicker]:
        """Attach only evidence that is shared by funding and spread paths.

        The source-specific parsers own raw vendor fields.  This single
        post-processing step applies the common base-unit and precision
        contract, records measured settlement intervals, and prevents the two
        strategies from drifting into separately invented metadata semantics.
        """
        enriched: dict[str, FundingTicker] = {}
        for key, ticker in tickers.items():
            interval_ms = self._observe_funding_interval(
                key,
                next_timestamp_ms=int(ticker.funding_timestamp_ms or 0),
                explicit_interval_ms=int(ticker.funding_interval_ms or 0),
                explicit_source=str(ticker.funding_interval_source or ""),
                explicit_observed_at_ms=int(
                    ticker.funding_interval_observed_at_ms or observed_at_ms
                ),
            )
            underlying, quote_currency = _canonical_contract_identity(
                self._spec.venue_id,
                ticker.symbol,
            )
            mark_index_source = ""
            if float(ticker.mark_price or 0.0) > 0.0 and float(ticker.index_price or 0.0) > 0.0:
                mark_index_source = "venue_mark_and_index"
            raw_price_tick = float(ticker.price_tick or 0.0)
            raw_quantity_step = float(ticker.quantity_step_base or 0.0)
            price_precision = _decimal_precision(raw_price_tick)
            quantity_precision = _decimal_precision(raw_quantity_step)
            # The parsers below only mark venues complete where their BBO
            # quantity is documented/converted into base units.  In
            # particular, an OKX/Bitget top size may be a contract count; a
            # static spec is not proof that it is comparable to base quantity.
            contract_type = "linear" if underlying and quote_currency else ""
            normalised_multiplier = 1.0 if contract_type == "linear" else 0.0
            base_quantity_evidence = ticker.base_quantity_evidence is True
            complete_contract = bool(
                underlying
                and quote_currency
                and contract_type
                and normalised_multiplier > 0.0
                and mark_index_source
                # Zero decimal places is valid precision (for example a
                # quantity step of 1.0).  Presence is proved by the positive
                # raw exchange increments, not by overloading zero as a
                # missing-value sentinel.
                and raw_price_tick > 0.0
                and raw_quantity_step > 0.0
                and math.isfinite(raw_price_tick)
                and math.isfinite(raw_quantity_step)
                and float(ticker.min_quantity_base or 0.0) > 0.0
                and math.isfinite(float(ticker.min_notional_quote or 0.0))
                and float(ticker.min_notional_quote or 0.0) >= 0.0
                and ticker.min_notional_evidence_complete is True
                and price_precision >= 0
                and quantity_precision >= 0
                and base_quantity_evidence
            )
            enriched[key] = replace(
                ticker,
                funding_interval_ms=interval_ms,
                underlying=underlying,
                quote_currency=quote_currency,
                contract_type=contract_type,
                contract_multiplier=normalised_multiplier,
                mark_index_source=mark_index_source,
                price_precision=price_precision,
                quantity_precision=quantity_precision,
                venue_status="active" if complete_contract else "unknown",
                contract_normalization_complete=complete_contract,
            )
        return enriched

    def _observe_funding_interval(
        self,
        key: str,
        *,
        next_timestamp_ms: int,
        explicit_interval_ms: int,
        explicit_source: str = "",
        explicit_observed_at_ms: int = 0,
    ) -> int:
        """Keep a measured interval cache without assuming an 8-hour cadence."""
        now_ms = _now_ms()
        explicit = max(int(explicit_interval_ms or 0), 0)
        source = str(explicit_source or "").strip()
        evidence_at_ms = max(int(explicit_observed_at_ms or 0), 0)
        if explicit > 0 and source and evidence_at_ms > 0:
            self._funding_interval_by_key[key] = (explicit, source, evidence_at_ms)
        # Hyperliquid's venue protocol is explicitly hourly; this is not the
        # old cross-venue default and remains tagged by its source parser.
        elif self._spec.venue_id == Venue.HYPERLIQUID:
            self._funding_interval_by_key[key] = (
                3_600_000,
                "hyperliquid_protocol_hourly",
                now_ms,
            )
        observed = max(int(next_timestamp_ms or 0), 0)
        previous = int(self._funding_schedule_next_by_key.get(key, 0) or 0)
        if observed > 0:
            if previous > 0 and observed > previous:
                advanced_by = observed - previous
                # A malformed timestamp must not poison the cache.  Perpetual
                # funding cadences outside this bounded range are unsupported
                # until directly supplied by the venue.
                if 60_000 <= advanced_by <= 7 * 24 * 60 * 60 * 1_000:
                    self._funding_interval_by_key[key] = (
                        advanced_by,
                        "observed_next_funding_transition",
                        now_ms,
                    )
            self._funding_schedule_next_by_key[key] = observed
        evidence = self._funding_interval_by_key.get(key)
        if evidence is None:
            return 0
        if not isinstance(evidence, tuple) or len(evidence) != 3:
            # The cache is not an authority boundary.  Corrupt or legacy
            # in-memory values must degrade this symbol, never crash the whole
            # venue refresh or regain the old unproven interval assumption.
            self._funding_interval_by_key.pop(key, None)
            return 0
        interval_ms, _, interval_observed_at_ms = evidence
        max_age_ms = min(max(2 * interval_ms, 3_600_000), 86_400_000)
        age_ms = now_ms - interval_observed_at_ms
        if age_ms < 0 or age_ms > max_age_ms:
            self._funding_interval_by_key.pop(key, None)
            return 0
        return max(int(interval_ms or 0), 0)

    def prime_funding_interval_evidence(
        self,
        evidence_by_key: dict[str, tuple[int, str, int]],
    ) -> None:
        """Restore persisted, exchange-derived funding cadence evidence.

        The cache deliberately retains the original evidence timestamp.  A
        restart is not fresh exchange evidence and must not extend the normal
        cadence validity window.
        """
        venue_prefix = f"{self._spec.venue_id.value}:"
        for raw_key, raw_evidence in evidence_by_key.items():
            key = str(raw_key or "").strip()
            if not key.startswith(venue_prefix):
                continue
            symbol = key.removeprefix(venue_prefix).strip().upper()
            if not symbol or not isinstance(raw_evidence, tuple) or len(raw_evidence) != 3:
                continue
            interval_ms, source, observed_at_ms = raw_evidence
            if (
                isinstance(interval_ms, bool)
                or not isinstance(interval_ms, int)
                or not 60_000 <= interval_ms <= 7 * 24 * 60 * 60 * 1_000
                or not isinstance(source, str)
                or not source.strip()
                or isinstance(observed_at_ms, bool)
                or not isinstance(observed_at_ms, int)
                or observed_at_ms <= 0
            ):
                continue
            canonical_key = f"{venue_prefix}{symbol}"
            self._funding_interval_by_key[canonical_key] = (
                interval_ms,
                source.strip(),
                observed_at_ms,
            )

    def funding_interval_evidence(
        self,
        *,
        now_ms: int | None = None,
    ) -> dict[str, tuple[int, str, int]]:
        """Return still-valid cadence evidence for durable sidecar storage."""
        if now_ms is None:
            now_ms = _now_ms()
        return {
            key: evidence
            for key, evidence in self._funding_interval_by_key.items()
            if self._funding_interval_evidence_fresh(key, int(now_ms))
        }

    def _funding_interval_evidence_fresh(self, key: str, now_ms: int) -> bool:
        evidence = self._funding_interval_by_key.get(key)
        if evidence is None or not isinstance(evidence, tuple) or len(evidence) != 3:
            return False
        interval_ms, _, observed_at_ms = evidence
        max_age_ms = min(max(2 * int(interval_ms), 3_600_000), 86_400_000)
        age_ms = int(now_ms) - int(observed_at_ms)
        return int(interval_ms) > 0 and 0 <= age_ms <= max_age_ms

    async def _fetch_binance_style_funding_intervals(
        self,
        venue_sym_to_canon: dict[str, str],
        *,
        observed_at_ms: int,
    ) -> dict[str, tuple[int, str, int]]:
        """Cold-start interval proof from two venue funding settlements."""
        evidence: dict[str, tuple[int, str, int]] = {}
        missing = [
            venue_symbol
            for venue_symbol, canonical_symbol in venue_sym_to_canon.items()
            if not self._funding_interval_evidence_fresh(
                f"{self._spec.venue_id.value}:{canonical_symbol}",
                observed_at_ms,
            )
        ]
        if not missing:
            return evidence
        now_ms = _now_ms()
        last_refresh_at_ms = int(
            getattr(self, "_funding_interval_history_last_refresh_at_ms", 0) or 0
        )
        if 0 <= now_ms - last_refresh_at_ms < _FUNDING_INTERVAL_HISTORY_REFRESH_INTERVAL_MS:
            return evidence
        # Keep the cursor on the client rather than using set/dict iteration:
        # otherwise the first response-timeout window repeatedly starves the
        # same tail of the venue universe after every refresh.
        missing = sorted(dict.fromkeys(missing))
        cursor = int(getattr(self, "_funding_interval_history_refresh_cursor", 0) or 0)
        start = cursor % len(missing)
        cap = min(_FUNDING_INTERVAL_HISTORY_REFRESH_CAP, len(missing))
        selected = [
            missing[(start + offset) % len(missing)]
            for offset in range(cap)
        ]
        self._funding_interval_history_refresh_cursor = (start + cap) % len(missing)
        self._funding_interval_history_last_refresh_at_ms = now_ms
        semaphore = asyncio.Semaphore(_FUNDING_INTERVAL_HISTORY_SEMAPHORE)

        async def _one(venue_symbol: str) -> None:
            async with semaphore:
                try:
                    raw = await self._public_get(
                        "/fapi/v1/fundingRate",
                        params={"symbol": venue_symbol, "limit": 2},
                    )
                except PublicTransportError:
                    return
                rows = raw if isinstance(raw, list) else []
                timestamps = sorted(
                    {
                        int(_safe_float(row.get("fundingTime", 0)))
                        for row in rows
                        if isinstance(row, dict)
                        and _safe_float(row.get("fundingTime", 0)) > 0.0
                    }
                )
                if len(timestamps) < 2:
                    return
                interval_ms = timestamps[-1] - timestamps[-2]
                if 60_000 <= interval_ms <= 7 * 24 * 60 * 60 * 1_000:
                    evidence[venue_symbol] = (
                        interval_ms,
                        "venue_funding_history",
                        observed_at_ms,
                    )

        tasks = [asyncio.create_task(_one(symbol)) for symbol in selected]
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks),
                timeout=_FUNDING_INTERVAL_HISTORY_BUDGET_S,
            )
        except asyncio.TimeoutError:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        return evidence

    async def fetch_entry_open_interest_evidence(
        self,
        symbols: list[str],
        *,
        force_refresh: bool = True,
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
        if not scoped_symbols:
            return {}
        deduped = list(dict.fromkeys(scoped_symbols))
        phase_rows: list[dict[str, Any]] = []
        phase_token = _PUBLIC_REQUEST_PHASE_COLLECTOR.set(phase_rows)
        try:
            if self._spec.venue_id in (Venue.BINANCE, Venue.ASTER):
                result = await self._fetch_binance_style_entry_open_interest(
                    deduped,
                    force_refresh=force_refresh,
                )
            elif self._spec.venue_id == Venue.OKX:
                result = await self._fetch_okx_entry_open_interest(deduped)
            elif self._spec.venue_id == Venue.BYBIT:
                result = await self._fetch_bybit_entry_open_interest(deduped)
            elif self._spec.venue_id == Venue.BITGET:
                result = await self._fetch_bitget_entry_open_interest(deduped)
            elif self._spec.venue_id == Venue.GATE:
                result = await self._fetch_gate_entry_open_interest(deduped)
            elif self._spec.venue_id == Venue.HYPERLIQUID:
                result = await self._fetch_hyperliquid_entry_open_interest(deduped)
            else:
                result = {}
        finally:
            _PUBLIC_REQUEST_PHASE_COLLECTOR.reset(phase_token)
        phase_totals = {
            "oi_dns_ms": None,
            "oi_connect_ms": sum(int(row.get("connect_ms", 0) or 0) for row in phase_rows),
            "oi_pool_wait_ms": sum(int(row.get("pool_wait_ms", 0) or 0) for row in phase_rows),
            "oi_rate_limit_wait_ms": sum(
                int(row.get("rate_limit_wait_ms", 0) or 0) for row in phase_rows
            ),
            "oi_transport_total_ms": sum(
                int(row.get("transport_total_ms", 0) or 0) for row in phase_rows
            ),
            "oi_http_ms": sum(int(row.get("http_ms", 0) or 0) for row in phase_rows),
            "oi_parse_ms": sum(int(row.get("parse_ms", 0) or 0) for row in phase_rows),
            "oi_dns_timing_status": (
                "included_in_connect" if phase_rows else "unavailable"
            ),
        }
        return {
            key: replace(ticker, **phase_totals)
            for key, ticker in result.items()
        }

    def _entry_oi_ticker(
        self,
        *,
        canonical_symbol: str,
        venue_symbol: str,
        value_quote: float | None,
        raw_value: float | None,
        raw_unit: str,
        contract_multiplier: float | None,
        conversion_mark_price: float | None,
        observed_at_ms: int,
        received_at_ms: int,
        source: str,
        status: str,
        reason: str,
    ) -> FundingTicker:
        venue = self._spec.venue_id.value
        return FundingTicker(
            venue=venue,
            symbol=canonical_symbol,
            bid=0.0,
            ask=0.0,
            **_open_interest_fields(
                venue=venue,
                canonical_symbol=canonical_symbol,
                venue_symbol=venue_symbol,
                value_quote=value_quote,
                raw_value=raw_value,
                raw_unit=raw_unit,
                contract_multiplier=contract_multiplier,
                conversion_mark_price=conversion_mark_price,
                observed_at_ms=observed_at_ms,
                received_at_ms=received_at_ms,
                source=source,
                status=status,
                reason=reason,
            ),
        )

    def _entry_oi_unavailable_result(
        self,
        canonical_symbol: str,
        *,
        source: str,
        status: str,
        reason: str,
        received_at_ms: int | None = None,
    ) -> tuple[str, FundingTicker]:
        venue = self._spec.venue_id.value
        venue_symbol = self._to_venue_symbol(canonical_symbol)
        return f"{venue}:{canonical_symbol}", self._entry_oi_ticker(
            canonical_symbol=canonical_symbol,
            venue_symbol=venue_symbol,
            value_quote=None,
            raw_value=None,
            raw_unit="",
            contract_multiplier=None,
            conversion_mark_price=None,
            observed_at_ms=0,
            received_at_ms=received_at_ms or _now_ms(),
            source=source,
            status=status,
            reason=reason,
        )

    async def _run_isolated_entry_oi_request(
        self,
        canonical_symbol: str,
        *,
        source: str,
        fetch_one: Callable[[str], Awaitable[tuple[str, FundingTicker]]],
    ) -> tuple[str, FundingTicker]:
        """Keep one failed contract from poisoning the venue's whole batch."""
        try:
            return await fetch_one(canonical_symbol)
        except Exception as exc:
            status = self._binance_style_oi_status_from_error(exc)
            if isinstance(exc, (ValueError, TypeError, KeyError)):
                status = "parse_error"
            return self._entry_oi_unavailable_result(
                canonical_symbol,
                source=source,
                status=status,
                reason=_http_error_reason(exc),
            )

    async def _fetch_okx_entry_open_interest(
        self,
        symbols: list[str],
    ) -> dict[str, FundingTicker]:
        canonical_candidates: dict[str, tuple[str, ...]] = {}
        canonical_by_venue_symbol: dict[str, set[str]] = {}
        for raw_symbol in symbols:
            canonical = str(raw_symbol or "").strip().upper()
            if not canonical:
                continue
            # Targeted OI must use the same exact-unit identity rule as the
            # bulk funding and BBO paths.  Falling back from 1000BONK to BONK
            # would attach a one-unit contract's evidence to a different
            # canonical price unit.
            venue_candidates = (self._to_venue_symbol(canonical),)
            canonical_candidates[canonical] = venue_candidates
            for venue_symbol in venue_candidates:
                canonical_by_venue_symbol.setdefault(venue_symbol, set()).add(canonical)

        ambiguous_canonicals = {
            canonical
            for canonical, venue_candidates in canonical_candidates.items()
            if any(
                len(canonical_by_venue_symbol.get(venue_symbol, set())) > 1
                for venue_symbol in venue_candidates
            )
        }

        async def _one(canonical: str) -> tuple[str, FundingTicker]:
            venue_candidates = canonical_candidates.get(
                canonical,
                (self._to_venue_symbol(canonical),),
            )
            primary_venue_symbol = venue_candidates[0]
            if canonical in ambiguous_canonicals:
                return f"okx:{canonical}", self._entry_oi_ticker(
                    canonical_symbol=canonical,
                    venue_symbol=primary_venue_symbol,
                    value_quote=None,
                    raw_value=None,
                    raw_unit="",
                    contract_multiplier=None,
                    conversion_mark_price=None,
                    observed_at_ms=0,
                    received_at_ms=_now_ms(),
                    source="okx_contract_mapping",
                    status="ambiguous_mapping",
                    reason="multiple_canonical_symbols_for_venue_symbol",
                )

            async def _request(venue_symbol: str):
                raw = await self._public_get(
                    self._spec.open_interest_path,
                    params={"instType": "SWAP", "instId": venue_symbol},
                )
                return venue_symbol, raw, _now_ms()

            responses = await asyncio.gather(
                *(_request(venue_symbol) for venue_symbol in venue_candidates),
                return_exceptions=True,
            )
            valid_rows: list[tuple[str, dict[str, Any], int]] = []
            parse_failures: list[str] = []
            application_failures: list[tuple[str, str]] = []
            for response in responses:
                if isinstance(response, BaseException):
                    application_failures.append(
                        (
                            self._binance_style_oi_status_from_error(response),
                            _http_error_reason(response),
                        )
                    )
                    continue
                venue_symbol, raw, received_at_ms = response
                application_error = _entry_oi_application_error(Venue.OKX, raw)
                if application_error is not None:
                    application_failures.append(application_error)
                    continue
                if not isinstance(raw, dict) or not isinstance(raw.get("data"), list):
                    parse_failures.append("invalid_open_interest_response_shape")
                    continue
                matches = [
                    row
                    for row in raw.get("data", [])
                    if isinstance(row, dict)
                    and str(row.get("instId") or "") == venue_symbol
                ]
                if len(matches) > 1:
                    parse_failures.append("duplicate_instId")
                    valid_rows.extend(
                        (venue_symbol, row, received_at_ms) for row in matches
                    )
                elif len(matches) == 1:
                    valid_rows.append((venue_symbol, matches[0], received_at_ms))

            observed_at_ms = 0
            valid_received_at_ms = [item[2] for item in valid_rows]
            received_at_ms = (
                max(valid_received_at_ms)
                if valid_received_at_ms
                else _now_ms()
            )
            if application_failures and len(venue_candidates) > 1:
                # A prefixed canonical is only exact after every possible
                # venue symbol has been excluded or matched.  One successful
                # row plus one unknown request could still hide a second
                # listed contract, so it is not safe to bind either result.
                venue_symbol = primary_venue_symbol
                failure_priority = {
                    "rate_limited": 0,
                    "timeout": 1,
                    "http_error": 2,
                    "parse_error": 3,
                }
                status, reason = min(
                    application_failures,
                    key=lambda item: failure_priority.get(item[0], 9),
                )
                value = raw_value = None
                raw_unit = ""
            elif len(valid_rows) > 1:
                venue_symbol = primary_venue_symbol
                status, reason = "ambiguous_mapping", "multiple_matching_instId"
                value = raw_value = None
                raw_unit = ""
            elif not valid_rows:
                venue_symbol = primary_venue_symbol
                value = raw_value = None
                raw_unit = ""
                if "duplicate_instId" in parse_failures:
                    status, reason = "ambiguous_mapping", "duplicate_instId"
                elif parse_failures:
                    status, reason = "parse_error", parse_failures[0]
                elif application_failures and len(application_failures) == len(responses):
                    status, reason = application_failures[0]
                else:
                    status, reason = "symbol_not_listed", "instId_not_found"
            else:
                venue_symbol, row, received_at_ms = valid_rows[0]
                value, valid, key = _first_present_float(row, "oiUsd")
                if valid:
                    status, reason = "observed", key
                    raw_value, raw_unit = value, "quote"
                    (
                        observed_at_ms,
                        timestamp_status,
                        timestamp_reason,
                    ) = _entry_oi_observed_at_ms(
                        row.get("ts"),
                        received_at_ms=received_at_ms,
                        field_name="okx_open_interest_ts",
                    )
                    if timestamp_status is not None:
                        status = timestamp_status
                        reason = timestamp_reason or timestamp_status
                else:
                    status, reason = "parse_error", (
                        f"invalid_{key}" if key else "missing_oiUsd"
                    )
                    value = raw_value = None
                    raw_unit = ""
            return f"okx:{canonical}", self._entry_oi_ticker(
                canonical_symbol=canonical,
                venue_symbol=venue_symbol,
                value_quote=value,
                raw_value=raw_value,
                raw_unit=raw_unit,
                contract_multiplier=1.0 if status == "observed" else None,
                conversion_mark_price=None,
                observed_at_ms=observed_at_ms,
                received_at_ms=received_at_ms,
                source="okx_public_open_interest",
                status=status,
                reason=reason,
            )
        return dict(await asyncio.gather(*(
            self._run_isolated_entry_oi_request(
                symbol,
                source="okx_public_open_interest",
                fetch_one=_one,
            )
            for symbol in symbols
        )))

    async def _fetch_bybit_entry_open_interest(
        self,
        symbols: list[str],
    ) -> dict[str, FundingTicker]:
        async def _one(canonical: str) -> tuple[str, FundingTicker]:
            venue_symbol = self._to_venue_symbol(canonical)
            raw = await self._public_get(
                self._spec.funding_ticker_path or self._spec.market_snapshot_path,
                params={"category": "linear", "symbol": venue_symbol},
            )
            received_at_ms = _now_ms()
            application_error = _entry_oi_application_error(Venue.BYBIT, raw)
            if application_error is not None:
                status, reason = application_error
                return self._entry_oi_unavailable_result(
                    canonical,
                    source="bybit_tickers",
                    status=status,
                    reason=reason,
                    received_at_ms=received_at_ms,
                )
            payload = raw.get("result", {}) if isinstance(raw, dict) else {}
            if not isinstance(payload, dict) or not isinstance(payload.get("list"), list):
                return self._entry_oi_unavailable_result(
                    canonical,
                    source="bybit_tickers",
                    status="parse_error",
                    reason="invalid_ticker_response_shape",
                    received_at_ms=received_at_ms,
                )
            rows = payload.get("list", []) if isinstance(payload, dict) else []
            rows = rows if isinstance(rows, list) else []
            observed_at_ms = 0
            matches = [
                row for row in rows
                if isinstance(row, dict) and str(row.get("symbol") or "") == venue_symbol
            ]
            if len(matches) > 1:
                status, reason = "ambiguous_mapping", "duplicate_symbol"
                value = raw_value = None
                raw_unit = ""
            elif not matches:
                status, reason = "symbol_not_listed", "symbol_not_found"
                value = raw_value = None
                raw_unit = ""
            else:
                value, raw_value, raw_unit, status, reason = (
                    _bybit_total_open_interest(matches[0])
                )
                if status == "observed":
                    (
                        observed_at_ms,
                        timestamp_status,
                        timestamp_reason,
                    ) = _entry_oi_observed_at_ms(
                        raw.get("time") if isinstance(raw, dict) else None,
                        received_at_ms=received_at_ms,
                        field_name="bybit_response_time",
                    )
                    if timestamp_status is not None:
                        status = timestamp_status
                        reason = timestamp_reason or timestamp_status
            return f"bybit:{canonical}", self._entry_oi_ticker(
                canonical_symbol=canonical,
                venue_symbol=venue_symbol,
                value_quote=value,
                raw_value=raw_value,
                raw_unit=raw_unit,
                contract_multiplier=1.0 if status == "observed" else None,
                conversion_mark_price=None,
                observed_at_ms=observed_at_ms,
                received_at_ms=received_at_ms,
                source="bybit_tickers",
                status=status,
                reason=reason,
            )
        return dict(await asyncio.gather(*(
            self._run_isolated_entry_oi_request(
                symbol,
                source="bybit_tickers",
                fetch_one=_one,
            )
            for symbol in symbols
        )))

    async def _fetch_bitget_entry_open_interest(
        self,
        symbols: list[str],
    ) -> dict[str, FundingTicker]:
        async def _one(canonical: str) -> tuple[str, FundingTicker]:
            venue_symbol = self._to_venue_symbol(canonical)
            oi_raw, ticker_raw = await asyncio.gather(
                self._public_get(
                    "/api/v3/market/open-interest",
                    params={
                        "category": "USDT-FUTURES",
                        "symbol": venue_symbol,
                    },
                ),
                self._public_get(
                    self._spec.market_snapshot_path,
                    params={
                        "productType": "USDT-FUTURES",
                        "symbol": venue_symbol,
                    },
                ),
            )
            received_at_ms = _now_ms()
            for response in (oi_raw, ticker_raw):
                application_error = _entry_oi_application_error(
                    Venue.BITGET,
                    response,
                )
                if application_error is not None:
                    status, reason = application_error
                    return self._entry_oi_unavailable_result(
                        canonical,
                        source="bitget_tickers",
                        status=status,
                        reason=reason,
                        received_at_ms=received_at_ms,
                    )
            oi_data = oi_raw.get("data", {}) if isinstance(oi_raw, dict) else {}
            if not isinstance(oi_data, dict) or not isinstance(oi_data.get("list"), list):
                return self._entry_oi_unavailable_result(
                    canonical,
                    source="bitget_tickers",
                    status="parse_error",
                    reason="invalid_open_interest_response_shape",
                    received_at_ms=received_at_ms,
                )
            oi_rows = oi_data.get("list", []) if isinstance(oi_data, dict) else []
            oi_rows = oi_rows if isinstance(oi_rows, list) else []
            ticker_data = (
                ticker_raw.get("data", []) if isinstance(ticker_raw, dict) else []
            )
            if not isinstance(ticker_data, (list, dict)):
                return self._entry_oi_unavailable_result(
                    canonical,
                    source="bitget_tickers",
                    status="parse_error",
                    reason="invalid_ticker_response_shape",
                    received_at_ms=received_at_ms,
                )
            ticker_rows = (
                ticker_data
                if isinstance(ticker_data, list)
                else [ticker_data] if isinstance(ticker_data, dict) else []
            )
            matches = [
                row for row in oi_rows
                if isinstance(row, dict) and str(row.get("symbol") or "") == venue_symbol
            ]
            ticker_matches = [
                row for row in ticker_rows
                if isinstance(row, dict) and str(row.get("symbol") or "") == venue_symbol
            ]
            value = raw_value = mark = None
            observed_at_ms = 0
            if len(matches) > 1 or len(ticker_matches) > 1:
                status, reason = "ambiguous_mapping", "duplicate_symbol"
            elif not matches or not ticker_matches:
                status, reason = "symbol_not_listed", "symbol_not_found"
            else:
                raw_value, valid, key = _first_present_float(
                    matches[0], "openInterest"
                )
                mark_value = _safe_float(ticker_matches[0].get("markPrice", 0))
                if valid and mark_value > 0.0:
                    mark = mark_value
                    value = raw_value * mark
                    status, reason = "observed", f"{key}_times_mark"
                    (
                        observed_at_ms,
                        timestamp_status,
                        timestamp_reason,
                    ) = _entry_oi_observed_at_ms(
                        oi_data.get("ts"),
                        received_at_ms=received_at_ms,
                        field_name="bitget_open_interest_ts",
                    )
                    if timestamp_status is not None:
                        status = timestamp_status
                        reason = timestamp_reason or timestamp_status
                else:
                    status = "parse_error"
                    reason = "missing_mark_price" if valid else (
                        f"invalid_{key}" if key else "missing_openInterest"
                    )
                    raw_value = None
            return f"bitget:{canonical}", self._entry_oi_ticker(
                canonical_symbol=canonical,
                venue_symbol=venue_symbol,
                value_quote=value,
                raw_value=raw_value,
                raw_unit="base" if status == "observed" else "",
                contract_multiplier=1.0 if status == "observed" else None,
                conversion_mark_price=mark,
                observed_at_ms=observed_at_ms,
                received_at_ms=received_at_ms,
                source="bitget_tickers",
                status=status,
                reason=reason,
            )
        return dict(await asyncio.gather(*(
            self._run_isolated_entry_oi_request(
                symbol,
                source="bitget_tickers",
                fetch_one=_one,
            )
            for symbol in symbols
        )))

    async def _fetch_gate_entry_open_interest(
        self,
        symbols: list[str],
    ) -> dict[str, FundingTicker]:
        async def _one(canonical: str) -> tuple[str, FundingTicker]:
            venue_symbol = self._to_venue_symbol(canonical)
            ticker_raw, contract_raw = await asyncio.gather(
                self._public_get(
                    self._spec.market_snapshot_path,
                    params={"contract": venue_symbol},
                ),
                self._public_get(
                    f"{self._spec.funding_contracts_path.rstrip('/')}"
                    f"/{venue_symbol}"
                ),
            )
            received_at_ms = _now_ms()
            for response in (ticker_raw, contract_raw):
                application_error = _entry_oi_application_error(
                    Venue.GATE,
                    response,
                )
                if application_error is not None:
                    status, reason = application_error
                    return self._entry_oi_unavailable_result(
                        canonical,
                        source="gate_tickers",
                        status=status,
                        reason=reason,
                        received_at_ms=received_at_ms,
                    )
            if not isinstance(ticker_raw, list) or not isinstance(contract_raw, dict):
                return self._entry_oi_unavailable_result(
                    canonical,
                    source="gate_tickers",
                    status="parse_error",
                    reason="invalid_open_interest_response_shape",
                    received_at_ms=received_at_ms,
                )
            ticker_rows = ticker_raw if isinstance(ticker_raw, list) else []
            contract_rows = [contract_raw] if isinstance(contract_raw, dict) else []
            matches = [
                row for row in ticker_rows
                if isinstance(row, dict) and str(row.get("contract") or "") == venue_symbol
            ]
            contract_matches = [
                row for row in contract_rows
                if isinstance(row, dict) and str(row.get("name") or "") == venue_symbol
            ]
            value = raw_value = multiplier = mark = None
            if len(matches) > 1 or len(contract_matches) > 1:
                status, reason = "ambiguous_mapping", "duplicate_contract"
            elif not matches or not contract_matches:
                status, reason = "symbol_not_listed", "contract_not_found"
            else:
                row = matches[0]
                raw_value, valid, key = _first_present_float(row, "total_size")
                multiplier_value = _positive_exchange_number(
                    contract_matches[0].get("quanto_multiplier")
                )
                mark_value = _safe_float(row.get("mark_price", 0))
                if valid and multiplier_value > 0.0 and mark_value > 0.0:
                    multiplier, mark = multiplier_value, mark_value
                    value = raw_value * multiplier * mark
                    status, reason = "observed", f"{key}_times_quanto_mark"
                else:
                    status = "parse_error"
                    if not valid:
                        reason = f"invalid_{key}" if key else "missing_total_size"
                    elif multiplier_value <= 0.0:
                        reason = "missing_contract_multiplier"
                    else:
                        reason = "missing_mark_price"
                    raw_value = None
            return f"gate:{canonical}", self._entry_oi_ticker(
                canonical_symbol=canonical,
                venue_symbol=venue_symbol,
                value_quote=value,
                raw_value=raw_value,
                raw_unit="contracts" if status == "observed" else "",
                contract_multiplier=multiplier,
                conversion_mark_price=mark,
                # Gate does not return a native OI event timestamp.  Zero is
                # explicit unknown; local observation/receipt remains the
                # response-arrival clock below.
                observed_at_ms=0,
                received_at_ms=received_at_ms,
                source="gate_tickers",
                status=status,
                reason=reason,
            )
        return dict(await asyncio.gather(*(
            self._run_isolated_entry_oi_request(
                symbol,
                source="gate_tickers",
                fetch_one=_one,
            )
            for symbol in symbols
        )))

    async def _fetch_hyperliquid_entry_open_interest(
        self,
        symbols: list[str],
    ) -> dict[str, FundingTicker]:
        raw, received_at_ms = await self._public_post_with_received_at(
            self._spec.market_snapshot_path,
            body={"type": "metaAndAssetCtxs"},
        )
        application_error = _entry_oi_application_error(Venue.HYPERLIQUID, raw)
        if application_error is not None:
            status, reason = application_error
            return dict(
                self._entry_oi_unavailable_result(
                    canonical,
                    source="hyperliquid_meta_and_asset_ctxs",
                    status=status,
                    reason=reason,
                    received_at_ms=received_at_ms,
                )
                for canonical in symbols
            )
        if (
            not isinstance(raw, list)
            or len(raw) < 2
            or not isinstance(raw[0], dict)
            or not isinstance(raw[1], list)
        ):
            return dict(
                self._entry_oi_unavailable_result(
                    canonical,
                    source="hyperliquid_meta_and_asset_ctxs",
                    status="parse_error",
                    reason="invalid_meta_and_asset_contexts_shape",
                    received_at_ms=received_at_ms,
                )
                for canonical in symbols
            )
        meta = raw[0] if isinstance(raw, list) and len(raw) > 0 and isinstance(raw[0], dict) else {}
        contexts = raw[1] if isinstance(raw, list) and len(raw) > 1 and isinstance(raw[1], list) else []
        universe = meta.get("universe", []) if isinstance(meta, dict) else []
        rows: dict[str, dict[str, Any]] = {}
        for index, item in enumerate(universe if isinstance(universe, list) else []):
            if not isinstance(item, dict):
                continue
            venue_symbol = str(item.get("name") or "")
            if not venue_symbol or index >= len(contexts) or not isinstance(contexts[index], dict):
                continue
            if venue_symbol in rows:
                rows[venue_symbol] = {"_ambiguous": True}
            else:
                rows[venue_symbol] = contexts[index]
        result: dict[str, FundingTicker] = {}
        for canonical in symbols:
            venue_symbol = self._to_venue_symbol(canonical)
            ctx = rows.get(venue_symbol)
            value = raw_value = mark = None
            if ctx is None:
                status, reason = "symbol_not_listed", "asset_not_found"
            elif ctx.get("_ambiguous"):
                status, reason = "ambiguous_mapping", "duplicate_asset"
            else:
                raw_value, valid, key = _first_present_float(ctx, "openInterest")
                mark_value = _safe_float(ctx.get("markPx", ctx.get("oraclePx", 0)))
                if valid and mark_value > 0.0:
                    mark = mark_value
                    value = raw_value * mark
                    status, reason = "observed", f"{key}_times_mark"
                else:
                    status = "parse_error"
                    reason = "missing_mark_price" if valid else (
                        f"invalid_{key}" if key else "missing_openInterest"
                    )
                    raw_value = None
            result[f"hyperliquid:{canonical}"] = self._entry_oi_ticker(
                canonical_symbol=canonical,
                venue_symbol=venue_symbol,
                value_quote=value,
                raw_value=raw_value,
                raw_unit="base" if status == "observed" else "",
                contract_multiplier=1.0 if status == "observed" else None,
                conversion_mark_price=mark,
                # Hyperliquid's combined context has no native OI event time.
                observed_at_ms=0,
                received_at_ms=received_at_ms,
                source="hyperliquid_meta_and_asset_ctxs",
                status=status,
                reason=reason,
            )
        return result

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
                        # Keep every Gate L2 snapshot caller in the same
                        # sequence domain as futures.order_book_update.
                        params["with_id"] = "true"
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
        status_code = int(getattr(exc, "status_code", 0) or 0)
        message = str(exc).lower()
        if "timeout" in message:
            return "timeout"
        if status_code in (429, 418) or "429" in message or "too many" in message:
            return "rate_limited"
        if status_code in (400, 404) and (
            "invalid symbol" in message
            or "unknown symbol" in message
            or "symbol not found" in message
            or "-1121" in message
        ):
            return "symbol_not_listed"
        return "http_error"

    def _binance_style_oi_cache_key(self, venue_sym: str) -> str:
        return f"{self._spec.venue_id.value}:{venue_sym}"

    def _binance_style_cached_open_interest(
        self,
        venue_sym: str,
        *,
        mark_price: float | None,
        now_ms: int,
    ) -> tuple[float, int, str, str] | None:
        entry = self._binance_style_open_interest_cache.get(
            self._binance_style_oi_cache_key(venue_sym)
        )
        if entry is None:
            return None
        open_interest_quote, cached_mark_price, observed_at_ms, status, reason = entry
        if now_ms - int(observed_at_ms or 0) > BINANCE_STYLE_OPEN_INTEREST_CACHE_MAX_AGE_MS:
            return None
        if cached_mark_price <= 0.0:
            return None
        if mark_price is not None and mark_price <= 0.0:
            return None
        return open_interest_quote, int(observed_at_ms), status, reason

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
        if (
            normalize_open_interest_status(status)
            != OpenInterestEvidenceStatus.OBSERVED.value
            or not math.isfinite(open_interest_quote)
            or open_interest_quote < 0.0
            or mark_price <= 0.0
            or observed_at_ms <= 0
        ):
            return
        self._binance_style_open_interest_cache[
            self._binance_style_oi_cache_key(venue_sym)
        ] = (open_interest_quote, mark_price, observed_at_ms, status, reason)

    @staticmethod
    def _binance_style_first_symbol_item(
        raw: Any,
        venue_sym: str,
    ) -> dict[str, Any] | None:
        if isinstance(raw, dict):
            if not raw:
                return None
            data = raw.get("data")
            if isinstance(data, list):
                items = data
            else:
                items = [raw]
        elif isinstance(raw, list):
            items = raw
        else:
            return None
        symbol_free_items: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol", "") or item.get("s", "") or "")
            if symbol == venue_sym:
                return item
            if not symbol:
                symbol_free_items.append(item)
        # A per-symbol endpoint is allowed to omit the symbol only when its
        # response is unambiguously a single row.  Never bind the first row of
        # a multi-symbol response to the requested contract.
        if len(items) == 1 and len(symbol_free_items) == 1:
            return symbol_free_items[0]
        return None

    @staticmethod
    def _binance_style_parse_required_float(
        item: dict[str, Any],
        *keys: str,
    ) -> tuple[float, bool, str]:
        for key in keys:
            if not _has_nonempty_field(item, key):
                continue
            value = item.get(key)
            if isinstance(value, bool):
                return 0.0, False, key
            try:
                parsed = float(value)
            except (TypeError, ValueError, OverflowError):
                return 0.0, False, key
            if not math.isfinite(parsed) or parsed < 0.0:
                return 0.0, False, key
            return parsed, True, key
        return 0.0, False, ""

    async def _fetch_binance_style_open_interest_probe(
        self,
        venue_sym: str,
        *,
        mark_price: float | None,
        observed_at_ms: int,
    ) -> tuple[str, float, float, int, str, str]:
        spec = self._spec
        async with self._binance_style_open_interest_semaphore:
            if not spec.open_interest_path:
                return venue_sym, 0.0, 0.0, observed_at_ms, "unsupported", "unsupported"

            resolved_mark = float(mark_price or 0.0)
            if resolved_mark <= 0.0 and spec.premium_index_path:
                try:
                    raw_pi = await self._public_get(
                        spec.premium_index_path,
                        params={"symbol": venue_sym},
                    )
                except Exception as exc:
                    status = self._binance_style_oi_status_from_error(exc)
                    if status == "symbol_not_listed":
                        return (
                            venue_sym,
                            0.0,
                            0.0,
                            observed_at_ms,
                            "symbol_not_listed",
                            "premium_index_symbol_rejected_before_oi_http",
                        )
                    return (
                        venue_sym,
                        0.0,
                        0.0,
                        observed_at_ms,
                        status,
                        _http_error_reason(exc),
                    )
                pi_item = self._binance_style_first_symbol_item(raw_pi, venue_sym)
                if pi_item is None:
                    return (
                        venue_sym,
                        0.0,
                        0.0,
                        observed_at_ms,
                        "symbol_not_listed",
                        "missing_symbol_mark_before_http",
                    )
                resolved_mark, mark_ok, _mark_key = self._binance_style_parse_required_float(
                    pi_item,
                    "markPrice",
                    "mark_price",
                    "markPx",
                )
                if not mark_ok or resolved_mark <= 0.0:
                    return (
                        venue_sym,
                        0.0,
                        0.0,
                        observed_at_ms,
                        "missing_mark_price",
                        "missing_mark_price",
                    )

            if resolved_mark <= 0.0:
                return (
                    venue_sym,
                    0.0,
                    0.0,
                    observed_at_ms,
                    "missing_mark_price",
                    "missing_mark_price",
                )

            try:
                raw_oi = await self._public_get(
                    spec.open_interest_path,
                    params={"symbol": venue_sym},
                )
            except Exception as exc:
                return (
                    venue_sym,
                    0.0,
                    resolved_mark,
                    observed_at_ms,
                    self._binance_style_oi_status_from_error(exc),
                    _http_error_reason(exc),
                )
            response_received_at_ms = _now_ms()
            oi_item = self._binance_style_first_symbol_item(raw_oi, venue_sym)
            if oi_item is None:
                return (
                    venue_sym,
                    0.0,
                    resolved_mark,
                    response_received_at_ms,
                    "parse_error",
                    "missing_open_interest",
                )
            open_interest, oi_ok, oi_key = self._binance_style_parse_required_float(
                oi_item,
                "openInterest",
                "open_interest",
                "oi",
                "size",
            )
            if not oi_ok:
                return (
                    venue_sym,
                    0.0,
                    resolved_mark,
                    response_received_at_ms,
                    "parse_error",
                    f"invalid_{oi_key or 'open_interest'}",
                )
            return (
                venue_sym,
                open_interest * resolved_mark,
                resolved_mark,
                response_received_at_ms,
                "observed",
                "fresh_refresh",
            )

    def _binance_style_record_open_interest_task(
        self,
        cache_key: str,
        task: asyncio.Task[tuple[str, float, float, int, str, str]],
    ) -> None:
        if self._binance_style_open_interest_inflight.get(cache_key) is task:
            self._binance_style_open_interest_inflight.pop(cache_key, None)
        try:
            result = task.result()
        except asyncio.CancelledError:
            return
        except Exception:
            return
        venue_sym, open_interest_quote, mark_price, observed_at_ms, status, reason = result
        self._binance_style_store_open_interest(
            venue_sym,
            open_interest_quote=open_interest_quote,
            mark_price=mark_price,
            observed_at_ms=observed_at_ms,
            status=status,
            reason=reason,
        )

    def _binance_style_open_interest_task(
        self,
        venue_sym: str,
        *,
        mark_price: float | None,
        observed_at_ms: int,
    ) -> asyncio.Task[tuple[str, float, float, int, str, str]]:
        cache_key = self._binance_style_oi_cache_key(venue_sym)
        task = self._binance_style_open_interest_inflight.get(cache_key)
        if task is not None and not task.done():
            return task
        self._binance_style_open_interest_inflight.pop(cache_key, None)
        task = asyncio.create_task(
            self._fetch_binance_style_open_interest_probe(
                venue_sym,
                mark_price=mark_price,
                observed_at_ms=observed_at_ms,
            ),
            name=venue_sym,
        )
        self._binance_style_open_interest_inflight[cache_key] = task
        task.add_done_callback(
            lambda completed, key=cache_key: self._binance_style_record_open_interest_task(
                key,
                completed,
            )
        )
        return task

    async def _fetch_binance_style_entry_open_interest(
        self,
        symbols: list[str],
        *,
        force_refresh: bool = True,
    ) -> dict[str, FundingTicker]:
        spec = self._spec
        venue_str = spec.venue_id.value
        now_ms = _now_ms()
        venue_sym_to_canon = {
            self._to_venue_symbol(symbol): symbol.upper()
            for symbol in list(dict.fromkeys(symbols))
        }
        result: dict[str, FundingTicker] = {}
        if not spec.open_interest_path:
            for venue_sym, canon in venue_sym_to_canon.items():
                result[f"{venue_str}:{canon}"] = FundingTicker(
                    venue=venue_str,
                    symbol=canon,
                    bid=0.0,
                    ask=0.0,
                    open_interest_evidence_status="unsupported",
                    open_interest_evidence_reason="unsupported",
                )
            return result

        oi_map: dict[str, tuple[float, float, int, str, str]] = {}
        pending_status: dict[str, tuple[str, str]] = {}
        tasks: list[asyncio.Task[tuple[str, float, float, int, str, str]]] = []
        for venue_sym in venue_sym_to_canon:
            cached = None
            if not force_refresh:
                cached = self._binance_style_cached_open_interest(
                    venue_sym,
                    mark_price=None,
                    now_ms=now_ms,
                )
            if cached is not None:
                open_interest_quote, observed_at_ms, status, reason = cached
                cached_mark = self._binance_style_open_interest_cache[
                    self._binance_style_oi_cache_key(venue_sym)
                ][1]
                oi_map[venue_sym] = (
                    open_interest_quote,
                    cached_mark,
                    observed_at_ms,
                    status,
                    reason or "cache_hit",
                )
                continue
            tasks.append(
                self._binance_style_open_interest_task(
                    venue_sym,
                    mark_price=None,
                    observed_at_ms=now_ms,
                )
            )

        refresh_started_ms = _now_ms()
        if tasks:
            budget_s = float(
                getattr(
                    self,
                    "binance_style_open_interest_enrichment_budget_s",
                    BINANCE_STYLE_ENTRY_OPEN_INTEREST_BUDGET_S,
                )
                or 0.0
            )
            done, pending = await asyncio.wait(tasks, timeout=max(budget_s, 0.0))
            for task in done:
                try:
                    venue_sym, open_interest_quote, mark_price, observed_at_ms, status, reason = (
                        task.result()
                    )
                except Exception:
                    continue
                if normalize_open_interest_status(status) == "observed":
                    oi_map[venue_sym] = (
                        open_interest_quote,
                        mark_price,
                        observed_at_ms,
                        status,
                        reason or "fresh_refresh",
                    )
                else:
                    pending_status[venue_sym] = (status, reason or status)
            for task in pending:
                venue_sym = task.get_name()
                pending_status[venue_sym] = ("timeout", "timeout_waiting_for_oi")
        elapsed_ms = max(_now_ms() - refresh_started_ms, 0)

        candidate_count = len(venue_sym_to_canon)
        cache_hit_count = candidate_count - len(tasks)
        filtered_before_http_count = sum(
            1
            for status_reason in pending_status.values()
            if normalize_open_interest_status(status_reason[0]) == "symbol_not_listed"
        )
        oi_refresh_attempt_count = max(len(tasks) - filtered_before_http_count, 0)
        for venue_sym, canon in venue_sym_to_canon.items():
            open_interest_quote, mark_price, observed_at_ms, status, reason = oi_map.get(
                venue_sym,
                (0.0, 0.0, 0, *pending_status.get(venue_sym, ("unavailable", "not_refreshed"))),
            )
            received_at_ms = observed_at_ms or now_ms
            result[f"{venue_str}:{canon}"] = FundingTicker(
                venue=venue_str,
                symbol=canon,
                bid=0.0,
                ask=0.0,
                mark_price=mark_price,
                **_open_interest_fields(
                    venue=venue_str,
                    canonical_symbol=canon,
                    venue_symbol=venue_sym,
                    value_quote=(
                        open_interest_quote
                        if normalize_open_interest_status(status) == "observed"
                        else None
                    ),
                    raw_value=(
                        open_interest_quote / mark_price
                        if mark_price > 0.0
                        and normalize_open_interest_status(status) == "observed"
                        else None
                    ),
                    raw_unit="base",
                    contract_multiplier=1.0,
                    conversion_mark_price=mark_price if mark_price > 0.0 else None,
                    observed_at_ms=observed_at_ms,
                    received_at_ms=received_at_ms,
                    source="binance_style_open_interest",
                    status=status,
                    reason=reason,
                ),
                oi_candidate_count=candidate_count,
                oi_cache_hit_count=cache_hit_count,
                oi_cache_miss_count=len(tasks),
                oi_refresh_attempt_count=oi_refresh_attempt_count,
                oi_refresh_cap=int(
                    getattr(
                        self,
                        "binance_style_open_interest_refresh_cap",
                        BINANCE_STYLE_OPEN_INTEREST_REFRESH_CAP,
                    )
                    or BINANCE_STYLE_OPEN_INTEREST_REFRESH_CAP
                ),
                oi_deferred_count=0,
                oi_timeout_count=sum(
                    1 for status_reason in pending_status.values() if status_reason[0] == "timeout"
                ),
                oi_refresh_elapsed_ms=elapsed_ms,
            )
        return result

    async def _fetch_binance_style(
        self,
        symbols: list[str],
        *,
        include_open_interest: bool = True,
    ) -> dict[str, FundingTicker]:
        spec = self._spec
        venue_str = spec.venue_id.value
        now_ms = _now_ms()
        venue_sym_to_canon: dict[str, str] = {}
        for s in symbols:
            venue_sym_to_canon[self._to_venue_symbol(s)] = s.upper()

        # 1. bookTicker (bid/ask).  Aster and Binance use this same venue-wide
        # endpoint for the sidecar scan.  Recycle a poisoned keep-alive pool
        # once on a statusless transport failure, while retaining the
        # configured per-request timeout and never retrying an HTTP response.
        raw_tickers = await self._public_get_with_recycled_transport_retry(
            spec.funding_ticker_path,
            attempt_timeout_s=self._exchange_http_timeout_ms / 1000.0,
        )
        market_received_at_ms = _now_ms()
        items = raw_tickers if isinstance(raw_tickers, list) else [raw_tickers]

        ticker_map: dict[str, dict] = {}
        for item in items:
            sym = str(item.get("symbol", ""))
            if sym in venue_sym_to_canon:
                ticker_map[sym] = item

        # Symbol-level exchangeInfo is the execution contract for price and
        # quantity increments.  Venue-wide defaults are configuration hints,
        # not proof that an individual perpetual is tradable with those units.
        contract_map: dict[str, dict[str, Any]] = {}
        missing_contract_metadata = False
        for venue_sym, canon in venue_sym_to_canon.items():
            cache_key = f"{venue_str}:{canon}"
            cached = self._binance_style_contract_metadata_by_key.get(cache_key)
            if cached is not None:
                metadata, metadata_observed_at_ms = cached
                metadata_age_ms = now_ms - int(metadata_observed_at_ms)
                underlying, quote_currency = _canonical_contract_identity(
                    spec.venue_id,
                    canon,
                )
                if (
                    0 <= metadata_age_ms <= _BINANCE_STYLE_CONTRACT_METADATA_MAX_AGE_MS
                    and _binance_style_contract_metadata_complete(
                        metadata,
                        underlying=underlying,
                        quote_currency=quote_currency,
                    )
                ):
                    contract_map[venue_sym] = dict(metadata)
                    continue
                if metadata_age_ms > _BINANCE_STYLE_CONTRACT_METADATA_MAX_AGE_MS:
                    self._binance_style_contract_metadata_by_key.pop(cache_key, None)
            missing_contract_metadata = True

        if spec.funding_contracts_path and missing_contract_metadata:
            try:
                exchange_info = await self._cached_public_get(
                    spec.funding_contracts_path,
                    max_age_ms=_BINANCE_STYLE_CONTRACT_METADATA_MAX_AGE_MS,
                )
            except PublicTransportError:
                exchange_info = {}
            contract_items = (
                exchange_info.get("symbols", [])
                if isinstance(exchange_info, dict)
                else []
            )
            for metadata in contract_items if isinstance(contract_items, list) else []:
                if not isinstance(metadata, dict):
                    continue
                venue_sym = str(metadata.get("symbol", "") or "")
                canon = venue_sym_to_canon.get(venue_sym)
                if canon is None:
                    continue
                cache_key = f"{venue_str}:{canon}"
                self._binance_style_contract_metadata_by_key[cache_key] = (
                    dict(metadata),
                    now_ms,
                )
                underlying, quote_currency = _canonical_contract_identity(
                    spec.venue_id,
                    canon,
                )
                if _binance_style_contract_metadata_complete(
                    metadata,
                    underlying=underlying,
                    quote_currency=quote_currency,
                ):
                    contract_map[venue_sym] = metadata

        # 2. premiumIndex (mark, index, funding rate, funding time)
        raw_pi: list[dict] = []
        funding_received_at_ms = 0
        if spec.premium_index_path:
            pi_resp = await self._public_get(spec.premium_index_path)
            funding_received_at_ms = _now_ms()
            raw_pi = pi_resp if isinstance(pi_resp, list) else [pi_resp]

        pi_map: dict[str, dict] = {}
        for item in raw_pi:
            sym = str(item.get("symbol", ""))
            if sym in venue_sym_to_canon:
                pi_map[sym] = item

        interval_evidence: dict[str, tuple[int, str, int]] = {}
        for venue_sym, pi in pi_map.items():
            explicit_hours = _safe_float(
                pi.get("fundingIntervalHours", pi.get("fundingInterval", 0))
            )
            if explicit_hours > 0.0:
                evidence = (
                    int(explicit_hours * 3_600_000),
                    "venue_premium_index_interval",
                    now_ms,
                )
                interval_evidence[venue_sym] = evidence
                canonical_symbol = venue_sym_to_canon[venue_sym]
                self._funding_interval_by_key[
                    f"{venue_str}:{canonical_symbol}"
                ] = evidence
        interval_evidence.update(
            await self._fetch_binance_style_funding_intervals(
                {
                    venue_sym: venue_sym_to_canon[venue_sym]
                    for venue_sym in pi_map
                },
                observed_at_ms=now_ms,
            )
        )

        # 3. 24hr (volume)
        vol_map: dict[str, float] = {}
        if spec.volume_24h_path:
            raw_24 = await self._cached_public_get(
                spec.volume_24h_path,
                max_age_ms=_SLOW_LIQUIDITY_SNAPSHOT_MAX_AGE_MS,
            )
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
        oi_observed_at_ms: dict[str, int] = {}
        oi_mark_price: dict[str, float] = {}
        oi_evidence_status: dict[str, str] = {
            venue_sym: "unavailable" if spec.open_interest_path else "unsupported"
            for venue_sym in venue_sym_to_canon
        }
        oi_evidence_reason: dict[str, str] = {
            venue_sym: "not_refreshed" if spec.open_interest_path else "unsupported"
            for venue_sym in venue_sym_to_canon
        }
        if spec.open_interest_path and include_open_interest:
            oi_candidate_count = 0
            oi_cache_hit_count = 0
            oi_cache_miss_count = 0
            oi_refresh_attempt_count = 0
            oi_deferred_count = 0
            oi_timeout_count = 0
            oi_refresh_elapsed_ms = 0

            oi_symbols: list[str] = []
            for sym in venue_sym_to_canon:
                if sym not in pi_map:
                    oi_evidence_status[sym] = "symbol_not_listed"
                    oi_evidence_reason[sym] = "missing_bulk_premium_index"
                    continue
                if sym not in ticker_map:
                    oi_evidence_status[sym] = "symbol_not_listed"
                    oi_evidence_reason[sym] = "missing_bulk_book_ticker"
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
                    oi_value, cached_observed_at_ms, status, reason = cached
                    oi_map[sym] = oi_value
                    oi_observed_at_ms[sym] = cached_observed_at_ms
                    oi_mark_price[sym] = self._binance_style_open_interest_cache[
                        self._binance_style_oi_cache_key(sym)
                    ][1]
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
            refresh_limit = max(refresh_cap, 0)
            if refresh_limit > 0 and len(oi_symbols) > refresh_limit:
                start = (
                    0
                    if oi_cache_hit_count > 0
                    else self._binance_style_open_interest_refresh_cursor % len(oi_symbols)
                )
                ordered = oi_symbols[start:] + oi_symbols[:start]
                refresh_symbols = ordered[:refresh_limit]
                refresh_set = set(refresh_symbols)
                deferred_symbols = [sym for sym in oi_symbols if sym not in refresh_set]
                self._binance_style_open_interest_refresh_cursor = (
                    start + len(refresh_symbols)
                ) % len(oi_symbols)
            else:
                refresh_symbols = oi_symbols[:refresh_limit] if refresh_limit > 0 else []
                refresh_set = set(refresh_symbols)
                deferred_symbols = [sym for sym in oi_symbols if sym not in refresh_set]
                if oi_symbols:
                    self._binance_style_open_interest_refresh_cursor = (
                        self._binance_style_open_interest_refresh_cursor
                        + len(refresh_symbols)
                    ) % len(oi_symbols)
            oi_refresh_attempt_count = len(refresh_symbols)
            for deferred_sym in deferred_symbols:
                oi_evidence_status[deferred_sym] = "deferred_by_cap"
                oi_evidence_reason[deferred_sym] = "refresh_cap_exceeded"
            oi_deferred_count = len(deferred_symbols)
            tasks = [
                self._binance_style_open_interest_task(
                    sym,
                    mark_price=_safe_float(pi_map.get(sym, {}).get("markPrice", 0)),
                    observed_at_ms=now_ms,
                )
                for sym in refresh_symbols
            ]
            if tasks:
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
                        venue_sym, open_interest_quote, mark_price, observed_at_ms, status, reason = (
                            task.result()
                        )
                    except Exception:
                        continue
                    oi_evidence_status[venue_sym] = status
                    oi_evidence_reason[venue_sym] = reason or status
                    if status == "timeout":
                        oi_timeout_count += 1
                    if normalize_open_interest_status(status) == "observed":
                        oi_map[venue_sym] = open_interest_quote
                        oi_observed_at_ms[venue_sym] = observed_at_ms
                        oi_mark_price[venue_sym] = mark_price
                        self._binance_style_store_open_interest(
                            venue_sym,
                            open_interest_quote=open_interest_quote,
                            mark_price=mark_price,
                            observed_at_ms=observed_at_ms,
                            status=status,
                            reason=reason or "fresh_refresh",
                        )
                for task in pending:
                    try:
                        venue_sym = task.get_name()
                    except Exception:
                        venue_sym = ""
                    if venue_sym:
                        oi_evidence_status[venue_sym] = "refresh_inflight"
                        oi_evidence_reason[venue_sym] = "background_refresh_inflight"
        else:
            oi_candidate_count = 0
            oi_cache_hit_count = 0
            oi_cache_miss_count = 0
            oi_refresh_attempt_count = 0
            refresh_cap = 0
            oi_deferred_count = 0
            oi_timeout_count = 0
            oi_refresh_elapsed_ms = 0
            if spec.open_interest_path and not include_open_interest:
                oi_evidence_status = {
                    venue_sym: "unavailable" for venue_sym in venue_sym_to_canon
                }
                oi_evidence_reason = {
                    venue_sym: "entry_targeted_revalidation_required"
                    for venue_sym in venue_sym_to_canon
                }

        result: dict[str, FundingTicker] = {}
        for venue_sym, canon in venue_sym_to_canon.items():
            t = ticker_map.get(venue_sym, {})
            pi = pi_map.get(venue_sym, {})
            if not pi and t:
                # V1 parity: bookTicker-only entries are not perpetual contracts.
                continue
            interval_ms, interval_source, interval_observed_at_ms = (
                interval_evidence.get(venue_sym, (0, "", 0))
            )
            contract_metadata = contract_map.get(venue_sym, {})
            underlying, quote_currency = _canonical_contract_identity(
                spec.venue_id,
                canon,
            )
            metadata_complete = _binance_style_contract_metadata_complete(
                contract_metadata,
                underlying=underlying,
                quote_currency=quote_currency,
            )
            price_tick, quantity_step = _binance_style_symbol_increments(
                contract_metadata
            )
            min_quantity, min_notional = _binance_style_symbol_minimums(
                contract_metadata
            )
            result[f"{venue_str}:{canon}"] = FundingTicker(
                venue=venue_str,
                symbol=canon,
                bid=_safe_float(t.get("bidPrice", 0)),
                ask=_safe_float(t.get("askPrice", 0)),
                market_received_at_ms=market_received_at_ms,
                bid_size=(
                    _safe_float(t.get("bidQty", 0)) if metadata_complete else 0.0
                ),
                ask_size=(
                    _safe_float(t.get("askQty", 0)) if metadata_complete else 0.0
                ),
                mark_price=_safe_float(pi.get("markPrice", 0)),
                index_price=_safe_float(pi.get("indexPrice", 0)),
                funding_rate_bps=_safe_float(pi.get("lastFundingRate", 0)) * 10000.0,
                **_funding_rate_fields(
                    venue=venue_str,
                    symbol=canon,
                    rate_bps=_safe_float(pi.get("lastFundingRate", 0)) * 10000.0,
                    funding_timestamp_ms=_funding_timestamp_ms(pi),
                    observed_at_ms=(funding_received_at_ms if pi else 0),
                    event_at_ms=int(_safe_float(pi.get("time", 0))),
                    source="binance_style_premium_index",
                ),
                settled_funding_rate_bps=(
                    _safe_float(pi.get("lastFundingRate", 0)) * 10000.0
                    if _has_nonempty_field(pi, "lastFundingRate")
                    else None
                ),
                predicted_funding_rate_bps=_optional_rate_bps(
                    pi, "nextFundingRate", "predictedFundingRate"
                ),
                funding_forecast_source=(
                    "venue_predicted_rate"
                    if _optional_rate_bps(pi, "nextFundingRate", "predictedFundingRate") is not None
                    else "quoted_rate"
                ),
                funding_timestamp_ms=int(_safe_float(pi.get("nextFundingTime", 0))),
                funding_interval_ms=interval_ms,
                funding_interval_source=interval_source,
                funding_interval_observed_at_ms=interval_observed_at_ms,
                volume_24h_quote=vol_map.get(venue_sym, 0.0),
                **_open_interest_fields(
                    venue=venue_str,
                    canonical_symbol=canon,
                    venue_symbol=venue_sym,
                    value_quote=oi_map.get(venue_sym),
                    raw_value=(
                        oi_map[venue_sym] / oi_mark_price[venue_sym]
                        if venue_sym in oi_map
                        and oi_mark_price.get(venue_sym, 0.0) > 0.0
                        else None
                    ),
                    raw_unit="base",
                    contract_multiplier=1.0,
                    conversion_mark_price=oi_mark_price.get(venue_sym) or None,
                    observed_at_ms=oi_observed_at_ms.get(venue_sym, 0),
                    received_at_ms=oi_observed_at_ms.get(venue_sym, now_ms),
                    source="binance_style_open_interest",
                    status=oi_evidence_status.get(
                        venue_sym,
                        "unavailable" if spec.open_interest_path else "unsupported",
                    ),
                    reason=oi_evidence_reason.get(venue_sym, ""),
                ),
                oi_candidate_count=oi_candidate_count,
                oi_cache_hit_count=oi_cache_hit_count,
                oi_cache_miss_count=oi_cache_miss_count,
                oi_refresh_attempt_count=oi_refresh_attempt_count,
                oi_refresh_cap=refresh_cap,
                oi_deferred_count=oi_deferred_count,
                oi_timeout_count=oi_timeout_count,
                oi_refresh_elapsed_ms=oi_refresh_elapsed_ms,
                base_quantity_evidence=metadata_complete,
                price_tick=price_tick if metadata_complete else 0.0,
                quantity_step_base=quantity_step if metadata_complete else 0.0,
                min_quantity_base=min_quantity if metadata_complete else 0.0,
                min_notional_quote=min_notional if metadata_complete else 0.0,
                min_notional_evidence_complete=metadata_complete,
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

    async def _fetch_okx_style(
        self,
        symbols: list[str],
        *,
        include_open_interest: bool = True,
    ) -> dict[str, FundingTicker]:
        spec = self._spec
        venue_str = spec.venue_id.value
        canonical_by_venue_symbol: dict[str, set[str]] = {}
        for raw_symbol in symbols:
            canonical = str(raw_symbol or "").strip().upper()
            if not canonical:
                continue
            # A canonical symbol defines the ticker price unit.  In
            # particular, OKX BONK-USDT-SWAP is quoted per BONK and cannot be
            # reused as 1000BONKUSDT merely because the underlying token is
            # the same.  Keep only exact wire mappings; a missing prefixed
            # wire contract is an unlisted alias, not a fallback candidate.
            venue_sym = self._to_venue_symbol(canonical)
            canonical_by_venue_symbol.setdefault(venue_sym, set()).add(canonical)
        ambiguous_mappings = {
            venue_symbol: canonical_symbols
            for venue_symbol, canonical_symbols in canonical_by_venue_symbol.items()
            if len(canonical_symbols) > 1
        }
        venue_sym_to_canon = {
            venue_symbol: next(iter(canonical_symbols))
            for venue_symbol, canonical_symbols in canonical_by_venue_symbol.items()
            if len(canonical_symbols) == 1
        }

        now_ms = _now_ms()

        # 1. market/tickers?instType=SWAP (bid/ask, volume, OI from volCcy24h)
        ticker_path = spec.funding_ticker_path
        raw = await self._public_get_with_recycled_transport_retry(
            ticker_path,
            params={"instType": "SWAP"},
            attempt_timeout_s=_OKX_MARKET_TICKERS_ATTEMPT_TIMEOUT_S,
        )
        market_received_at_ms = _now_ms()
        data = raw.get("data", [])
        items = data if isinstance(data, list) else [data]

        ticker_map: dict[str, dict] = {}
        for item in items:
            sym = str(item.get("instId", ""))
            if sym in venue_sym_to_canon:
                ticker_map[sym] = item

        # OKX BBO sizes for SWAP are contract counts.  Only symbol-level
        # instrument metadata can prove the base-quantity conversion.
        instrument_map: dict[str, dict[str, Any]] = {}
        try:
            instruments_raw = await self._cached_public_get(
                "/api/v5/public/instruments",
                params={"instType": "SWAP"},
                max_age_ms=_OKX_CONTRACT_METADATA_MAX_AGE_MS,
            )
        except PublicTransportError:
            instruments_raw = {}
        instrument_data = (
            instruments_raw.get("data", [])
            if isinstance(instruments_raw, dict)
            else []
        )
        for instrument in instrument_data if isinstance(instrument_data, list) else []:
            if not isinstance(instrument, dict):
                continue
            instrument_id = str(instrument.get("instId", "") or "")
            if instrument_id in venue_sym_to_canon:
                instrument_map[instrument_id] = instrument
                canonical = venue_sym_to_canon[instrument_id]
                self._okx_contract_metadata_by_key[f"{venue_str}:{canonical}"] = (
                    dict(instrument),
                    now_ms,
                )

        # Funding-rate responses and the local funding cache do not prove the
        # current mark or index price.  Fetch both from their dedicated OKX
        # public endpoints so a funding-cache hit cannot silently erase the
        # contract-normalisation evidence required by spread paper.
        mark_price_map: dict[str, float] = {}
        try:
            mark_raw = await self._public_get(
                "/api/v5/public/mark-price",
                params={"instType": "SWAP"},
            )
        except PublicTransportError:
            mark_raw = {}
        mark_data = mark_raw.get("data", []) if isinstance(mark_raw, dict) else []
        for mark_item in mark_data if isinstance(mark_data, list) else []:
            if not isinstance(mark_item, dict):
                continue
            instrument_id = str(mark_item.get("instId", "") or "")
            mark_price = _safe_float(mark_item.get("markPx", 0))
            if instrument_id in venue_sym_to_canon and mark_price > 0.0:
                mark_price_map[instrument_id] = mark_price

        index_price_map: dict[str, float] = {}
        quote_currencies = {
            _canonical_contract_identity(spec.venue_id, canon)[1]
            for canon in venue_sym_to_canon.values()
        }
        for quote_currency in sorted(quote_currencies):
            try:
                index_raw = await self._public_get(
                    "/api/v5/market/index-tickers",
                    params={"quoteCcy": quote_currency},
                )
            except PublicTransportError:
                continue
            index_data = (
                index_raw.get("data", []) if isinstance(index_raw, dict) else []
            )
            for index_item in index_data if isinstance(index_data, list) else []:
                if not isinstance(index_item, dict):
                    continue
                index_id = str(index_item.get("instId", "") or "")
                swap_id = f"{index_id}-SWAP"
                index_price = _safe_float(index_item.get("idxPx", 0))
                if swap_id in venue_sym_to_canon and index_price > 0.0:
                    index_price_map[swap_id] = index_price

        # 2. funding-rate with V1 parity cache.  OKX supports ``instId=ANY``
        # for the complete perpetual universe.  Prefer that single coherent
        # observation on a cold start, then fall back only for requested rows
        # absent from the batch response.  Per-symbol fan-out alone cannot
        # finish a production-sized universe inside the enrichment budget.
        funding_map: dict[str, dict] = {}
        funding_observed_at_ms: dict[str, int] = {}

        def _accept_funding_row(
            venue_sym: str,
            item: object,
            *,
            received_at_ms: int,
        ) -> bool:
            if not isinstance(item, dict):
                return False
            returned_symbol = str(item.get("instId", "") or "")
            if returned_symbol != venue_sym:
                return False
            funding_map[venue_sym] = item
            funding_observed_at_ms[venue_sym] = received_at_ms
            cache_key = f"{venue_str}:{venue_sym_to_canon[venue_sym]}"
            rate_bps = _safe_float(item.get("fundingRate", 0)) * 10000.0
            funding_time_ms = _funding_timestamp_ms_or_seconds(
                item.get("fundingTime", 0)
            )
            next_funding_time_ms = _funding_timestamp_ms(item)
            if next_funding_time_ms > 0:
                self._funding_cache[cache_key] = (
                    rate_bps,
                    next_funding_time_ms,
                    received_at_ms,
                )
            if next_funding_time_ms > funding_time_ms > 0:
                self._okx_funding_time_pair_by_key[cache_key] = (
                    funding_time_ms,
                    next_funding_time_ms,
                    received_at_ms,
                )
            else:
                self._okx_funding_time_pair_by_key.pop(cache_key, None)
            return True

        if spec.funding_rate_path:
            # Separate symbols into cache-hit (fresh) and cache-miss (need fetch)
            symbols_to_fetch: list[str] = []
            for venue_sym in venue_sym_to_canon:
                cache_key = f"{venue_str}:{venue_sym_to_canon[venue_sym]}"
                if self._funding_rate_is_fresh(cache_key, now_ms):
                    rate_bps, ts_ms, observed_at_ms = self._funding_cache[cache_key]
                    cached_row = {
                        "instId": venue_sym,
                        "fundingRate": str(rate_bps / 10000.0),
                        "nextFundingTime": str(ts_ms),
                    }
                    cached_pair = self._okx_funding_time_pair_by_key.get(
                        cache_key
                    )
                    if (
                        cached_pair is not None
                        and cached_pair[1] == ts_ms
                        and cached_pair[2] == observed_at_ms
                    ):
                        cached_row["fundingTime"] = str(cached_pair[0])
                    funding_map[venue_sym] = cached_row
                    funding_observed_at_ms[venue_sym] = observed_at_ms
                else:
                    symbols_to_fetch.append(venue_sym)

            if symbols_to_fetch:
                try:
                    batch = await asyncio.wait_for(
                        self._public_get(
                            spec.funding_rate_path,
                            params={"instId": "ANY"},
                        ),
                        timeout=_OKX_FUNDING_RATE_BATCH_TIMEOUT_S,
                    )
                except (PublicTransportError, asyncio.TimeoutError):
                    batch = {}
                batch_received_at_ms = _now_ms()
                batch_rows = batch.get("data", []) if isinstance(batch, dict) else []
                for item in batch_rows if isinstance(batch_rows, list) else []:
                    if not isinstance(item, dict):
                        continue
                    venue_sym = str(item.get("instId", "") or "")
                    if venue_sym not in venue_sym_to_canon:
                        continue
                    _accept_funding_row(
                        venue_sym,
                        item,
                        received_at_ms=batch_received_at_ms,
                    )
                symbols_to_fetch = [
                    venue_sym
                    for venue_sym in symbols_to_fetch
                    if venue_sym not in funding_map
                ]

            # Fetch only rows still missing after cache and batch hydration.
            if symbols_to_fetch:
                sem = asyncio.Semaphore(_OKX_FUNDING_RATE_SEMAPHORE)

                async def _fetch_funding(venue_sym: str) -> None:
                    async with sem:
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
                        if not isinstance(fr, dict):
                            return
                        fr_data = fr.get("data", [])
                        if not isinstance(fr_data, list) or not fr_data:
                            return
                        _accept_funding_row(
                            venue_sym,
                            fr_data[0],
                            received_at_ms=_now_ms(),
                        )

                # One failed batch must not fan out across the full production
                # universe.  Fresh cache rows are removed above, so successive
                # cycles can still hydrate a cold cache in bounded chunks.
                fallback_limit = max(
                    int(_OKX_FUNDING_RATE_FALLBACK_MAX_SYMBOLS),
                    1,
                )
                if len(symbols_to_fetch) > fallback_limit:
                    start = self._okx_funding_fallback_cursor % len(
                        symbols_to_fetch
                    )
                    rotated = symbols_to_fetch[start:] + symbols_to_fetch[:start]
                    symbols_to_fetch = rotated[:fallback_limit]
                    self._okx_funding_fallback_cursor = (
                        start + fallback_limit
                    ) % len(rotated)
                else:
                    self._okx_funding_fallback_cursor = 0
                tasks = [
                    asyncio.create_task(_fetch_funding(sym))
                    for sym in symbols_to_fetch
                ]
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True),
                        timeout=_OKX_FUNDING_RATE_FALLBACK_TOTAL_TIMEOUT_S,
                    )
                except asyncio.TimeoutError:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)

        # 3. open-interest?instType=SWAP
        oi_map: dict[str, float] = {}
        oi_raw_map: dict[str, float] = {}
        oi_raw_unit: dict[str, str] = {}
        oi_conversion_mark: dict[str, float | None] = {}
        oi_observed_at_ms: dict[str, int] = {}
        oi_evidence_status: dict[str, str] = {
            venue_sym: "unavailable" if spec.open_interest_path else "unsupported"
            for venue_sym in venue_sym_to_canon
        }
        oi_evidence_reason: dict[str, str] = {
            venue_sym: "not_refreshed" if spec.open_interest_path else "unsupported"
            for venue_sym in venue_sym_to_canon
        }
        if spec.open_interest_path and include_open_interest:
            try:
                oi_raw, oi_received_at_ms = await self._cached_public_get_with_received_at(
                    spec.open_interest_path,
                    params={"instType": "SWAP"},
                    max_age_ms=_SLOW_OPEN_INTEREST_SNAPSHOT_MAX_AGE_MS,
                )
                oi_data = oi_raw.get("data", [])
                items_oi = oi_data if isinstance(oi_data, list) else [oi_data]
                for item in items_oi:
                    if not isinstance(item, dict):
                        continue
                    sym = str(item.get("instId", ""))
                    if sym in venue_sym_to_canon:
                        quote_oi, has_quote_oi, quote_key = _first_present_float(item, "oiUsd")
                        if has_quote_oi:
                            oi_map[sym] = quote_oi
                            oi_raw_map[sym] = quote_oi
                            oi_raw_unit[sym] = "quote"
                            oi_conversion_mark[sym] = None
                            oi_observed_at_ms[sym] = oi_received_at_ms
                            oi_evidence_status[sym] = "observed"
                            oi_evidence_reason[sym] = quote_key
                            continue
                        raw_oi, has_raw_oi, raw_key = _first_present_float(
                            item,
                            "oiCcy",
                            "oi",
                        )
                        mark = mark_price_map.get(sym, 0.0)
                        if has_raw_oi and mark > 0.0:
                            oi_map[sym] = raw_oi * mark
                            oi_raw_map[sym] = raw_oi
                            oi_raw_unit[sym] = "base"
                            oi_conversion_mark[sym] = mark
                            oi_observed_at_ms[sym] = oi_received_at_ms
                            oi_evidence_status[sym] = "observed"
                            oi_evidence_reason[sym] = f"{raw_key}_times_mark"
                        elif has_raw_oi:
                            oi_evidence_status[sym] = "parse_error"
                            oi_evidence_reason[sym] = "missing_mark_price"
                        elif raw_key:
                            oi_evidence_status[sym] = "parse_error"
                            oi_evidence_reason[sym] = f"invalid_{raw_key}"
                for venue_sym in venue_sym_to_canon:
                    if venue_sym not in oi_map and oi_evidence_status.get(venue_sym) == "unavailable":
                        oi_evidence_reason[venue_sym] = "missing_open_interest"
            except PublicTransportError as exc:
                for venue_sym in venue_sym_to_canon:
                    oi_evidence_status[venue_sym] = "http_error"
                    oi_evidence_reason[venue_sym] = _http_error_reason(exc)

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
            instrument = instrument_map.get(venue_sym, {})
            underlying, quote_currency = _canonical_contract_identity(
                self._spec.venue_id,
                canon,
            )
            contract_value = _safe_float(instrument.get("ctVal", 0))
            contract_value_currency = str(
                instrument.get("ctValCcy", "") or ""
            ).strip().upper()
            metadata_complete = bool(
                contract_value > 0.0
                and str(instrument.get("ctType", "") or "").strip().lower()
                == "linear"
                and contract_value_currency == underlying
                and str(instrument.get("settleCcy", "") or "").strip().upper()
                == quote_currency
                and str(instrument.get("state", "") or "").strip().lower()
                == "live"
                and _safe_float(instrument.get("tickSz", 0)) > 0.0
                and _safe_float(instrument.get("lotSz", 0)) > 0.0
                and _safe_float(instrument.get("minSz", 0)) > 0.0
            )
            base_size_multiplier = contract_value if metadata_complete else 0.0
            funding_time_ms = _funding_timestamp_ms_or_seconds(
                fr.get("fundingTime", 0)
            )
            next_funding_time_ms = _funding_timestamp_ms(fr)
            explicit_interval_ms = (
                next_funding_time_ms - funding_time_ms
                if next_funding_time_ms > funding_time_ms > 0
                else 0
            )
            vol_ccy = _safe_float(t.get("volCcy24h", 0))
            last = _safe_float(t.get("last", 0))
            result[f"{venue_str}:{canon}"] = FundingTicker(
                venue=venue_str,
                symbol=canon,
                bid=_safe_float(t.get("bidPx", 0)),
                ask=_safe_float(t.get("askPx", 0)),
                market_received_at_ms=market_received_at_ms,
                market_event_at_ms=int(_safe_float(t.get("ts", 0))),
                bid_size=_safe_float(t.get("bidSz", 0)) * base_size_multiplier,
                ask_size=_safe_float(t.get("askSz", 0)) * base_size_multiplier,
                mark_price=mark_price_map.get(venue_sym, 0.0),
                index_price=index_price_map.get(venue_sym, 0.0),
                funding_rate_bps=_safe_float(fr.get("fundingRate", 0)) * 10000.0,
                **_funding_rate_fields(
                    venue=venue_str,
                    symbol=canon,
                    rate_bps=_safe_float(fr.get("fundingRate", 0)) * 10000.0,
                    funding_timestamp_ms=next_funding_time_ms,
                    observed_at_ms=funding_observed_at_ms.get(venue_sym, 0),
                    event_at_ms=int(_safe_float(fr.get("ts", 0))),
                    source="okx_funding_rate",
                ),
                predicted_funding_rate_bps=_optional_rate_bps(
                    fr, "nextFundingRate", "predictedFundingRate"
                ),
                funding_forecast_source=(
                    "venue_predicted_rate"
                    if _optional_rate_bps(fr, "nextFundingRate", "predictedFundingRate") is not None
                    else "quoted_rate"
                ),
                funding_timestamp_ms=next_funding_time_ms,
                funding_interval_ms=explicit_interval_ms,
                funding_interval_source=(
                    "okx_funding_time_pair" if explicit_interval_ms > 0 else ""
                ),
                funding_interval_observed_at_ms=(
                    funding_observed_at_ms.get(venue_sym, 0)
                    if explicit_interval_ms > 0
                    else 0
                ),
                volume_24h_quote=vol_ccy * last if vol_ccy > 0 and last > 0 else vol_ccy,
                **_open_interest_fields(
                    venue=venue_str,
                    canonical_symbol=canon,
                    venue_symbol=venue_sym,
                    value_quote=oi_map.get(venue_sym),
                    raw_value=oi_raw_map.get(venue_sym),
                    raw_unit=oi_raw_unit.get(venue_sym, ""),
                    contract_multiplier=1.0,
                    conversion_mark_price=oi_conversion_mark.get(venue_sym),
                    observed_at_ms=oi_observed_at_ms.get(venue_sym, 0),
                    received_at_ms=oi_observed_at_ms.get(venue_sym, now_ms),
                    source="okx_public_open_interest",
                    status=oi_evidence_status.get(
                        venue_sym,
                        "unavailable" if spec.open_interest_path else "unsupported",
                    ),
                    reason=oi_evidence_reason.get(venue_sym, ""),
                ),
                base_quantity_evidence=metadata_complete,
                price_tick=(
                    _safe_float(instrument.get("tickSz", 0))
                    if metadata_complete
                    else 0.0
                ),
                quantity_step_base=(
                    _safe_float(instrument.get("lotSz", 0)) * contract_value
                    if metadata_complete
                    else 0.0
                ),
                min_quantity_base=(
                    _safe_float(instrument.get("minSz", 0)) * contract_value
                    if metadata_complete
                    else 0.0
                ),
                min_notional_quote=(
                    0.0
                ),
                min_notional_evidence_complete=metadata_complete,
            )
        for venue_sym, canonical_symbols in ambiguous_mappings.items():
            for canon in sorted(canonical_symbols):
                result[f"{venue_str}:{canon}"] = FundingTicker(
                    venue=venue_str,
                    symbol=canon,
                    bid=0.0,
                    ask=0.0,
                    market_received_at_ms=market_received_at_ms,
                    mark_price=0.0,
                    index_price=0.0,
                    funding_rate_bps=0.0,
                    funding_timestamp_ms=0,
                    **_open_interest_fields(
                        venue=venue_str,
                        canonical_symbol=canon,
                        venue_symbol=venue_sym,
                        value_quote=None,
                        raw_value=None,
                        raw_unit="",
                        contract_multiplier=None,
                        conversion_mark_price=None,
                        observed_at_ms=0,
                        received_at_ms=now_ms,
                        source="okx_contract_mapping",
                        status="ambiguous_mapping",
                        reason="multiple_canonical_symbols_for_venue_symbol",
                    ),
                )
        return result

    # -- Bybit -------------------------------------------------------------

    async def _fetch_bybit_style(self, symbols: list[str]) -> dict[str, FundingTicker]:
        spec = self._spec
        venue_str = spec.venue_id.value
        venue_sym_to_canon: dict[str, str] = {}
        for s in symbols:
            venue_sym_to_canon[self._to_venue_symbol(s)] = s.upper()

        raw, market_received_at_ms = await self._public_get_with_received_at(
            spec.funding_ticker_path,
            params={"category": "linear"},
        )
        result_wrap = raw.get("result", raw)
        items = result_wrap.get("list", []) if isinstance(result_wrap, dict) else (result_wrap if isinstance(result_wrap, list) else [])

        instrument_map: dict[str, dict[str, Any]] = {}
        if spec.funding_contracts_path:
            cursor = ""
            seen_cursors: set[str] = set()
            metadata_failed = False
            for _page in range(10):
                params = {"category": "linear", "limit": 1000}
                if cursor:
                    params["cursor"] = cursor
                try:
                    instruments_raw = await self._cached_public_get(
                        spec.funding_contracts_path,
                        params=params,
                        max_age_ms=_OKX_CONTRACT_METADATA_MAX_AGE_MS,
                    )
                except PublicTransportError:
                    metadata_failed = True
                    break
                instruments_wrap = (
                    instruments_raw.get("result", instruments_raw)
                    if isinstance(instruments_raw, dict)
                    else instruments_raw
                )
                instrument_items = (
                    instruments_wrap.get("list", [])
                    if isinstance(instruments_wrap, dict)
                    else instruments_wrap if isinstance(instruments_wrap, list) else []
                )
                for instrument in instrument_items:
                    if isinstance(instrument, dict):
                        instrument_map[str(instrument.get("symbol", ""))] = instrument
                next_cursor = (
                    str(instruments_wrap.get("nextPageCursor", "") or "")
                    if isinstance(instruments_wrap, dict)
                    else ""
                )
                if not next_cursor:
                    break
                if next_cursor == cursor or next_cursor in seen_cursors:
                    metadata_failed = True
                    break
                seen_cursors.add(next_cursor)
                cursor = next_cursor
            else:
                metadata_failed = True
            if metadata_failed:
                # A partial instrument universe is not a complete execution
                # contract.  Never mix first-page proof with an unknown tail.
                instrument_map.clear()

        result: dict[str, FundingTicker] = {}
        now_ms = _now_ms()
        for item in items:
            sym = str(item.get("symbol", ""))
            canon = venue_sym_to_canon.get(sym)
            if canon is None:
                continue
            (
                open_interest_quote,
                raw_open_interest,
                raw_open_interest_unit,
                oi_status,
                oi_reason,
            ) = _bybit_total_open_interest(
                item
            )
            has_open_interest_quote = oi_status == "observed"
            instrument = instrument_map.get(sym, {})
            price_filter = instrument.get("priceFilter", {})
            lot_size_filter = instrument.get("lotSizeFilter", {})
            if not isinstance(price_filter, dict):
                price_filter = {}
            if not isinstance(lot_size_filter, dict):
                lot_size_filter = {}
            funding_interval_minutes = _safe_float(
                instrument.get("fundingInterval", 0)
            )
            explicit_interval_ms = (
                int(funding_interval_minutes * 60_000)
                if funding_interval_minutes > 0
                else 0
            )
            metadata_complete = bool(
                instrument
                and str(instrument.get("status", "")).lower() == "trading"
                and str(instrument.get("contractType", "")).lower()
                in {"linearperpetual", "perpetual"}
                and str(instrument.get("settleCoin", "")).upper() == "USDT"
                and _safe_float(price_filter.get("tickSize", 0)) > 0
                and _safe_float(lot_size_filter.get("qtyStep", 0)) > 0
                and _safe_float(lot_size_filter.get("minOrderQty", 0)) > 0
                and _positive_exchange_number(
                    lot_size_filter.get("minNotionalValue")
                ) > 0.0
            )
            funding_timestamp_ms = _funding_timestamp_ms(item, fallback_ms=0)
            if funding_timestamp_ms <= now_ms + _FUNDING_CACHE_MIN_FUTURE_MS:
                funding_timestamp_ms = 0
            result[f"{venue_str}:{canon}"] = FundingTicker(
                venue=venue_str,
                symbol=canon,
                bid=_safe_float(item.get("bid1Price", 0)),
                ask=_safe_float(item.get("ask1Price", 0)),
                market_received_at_ms=market_received_at_ms,
                market_event_at_ms=int(
                    _safe_float(raw.get("time", 0)) if isinstance(raw, dict) else 0
                ),
                bid_size=(
                    _safe_float(item.get("bid1Size", 0))
                    if metadata_complete
                    else 0.0
                ),
                ask_size=(
                    _safe_float(item.get("ask1Size", 0))
                    if metadata_complete
                    else 0.0
                ),
                mark_price=_safe_float(item.get("markPrice", 0)),
                index_price=_safe_float(item.get("indexPrice", 0)),
                funding_rate_bps=_safe_float(item.get("fundingRate", 0)) * 10000.0,
                **_funding_rate_fields(
                    venue=venue_str,
                    symbol=canon,
                    rate_bps=_safe_float(item.get("fundingRate", 0)) * 10000.0,
                    funding_timestamp_ms=funding_timestamp_ms,
                    observed_at_ms=market_received_at_ms,
                    event_at_ms=int(
                        _safe_float(raw.get("time", 0))
                        if isinstance(raw, dict)
                        else 0
                    ),
                    source="bybit_linear_tickers",
                ),
                predicted_funding_rate_bps=_optional_rate_bps(
                    item, "predictedFundingRate", "nextFundingRate"
                ),
                funding_forecast_source=(
                    "venue_predicted_rate"
                    if _optional_rate_bps(item, "predictedFundingRate", "nextFundingRate") is not None
                    else "quoted_rate"
                ),
                funding_timestamp_ms=funding_timestamp_ms,
                funding_interval_ms=explicit_interval_ms,
                funding_interval_source=(
                    "bybit_instrument_metadata" if explicit_interval_ms > 0 else ""
                ),
                funding_interval_observed_at_ms=(
                    now_ms if explicit_interval_ms > 0 else 0
                ),
                volume_24h_quote=_safe_float(item.get("turnover24h", 0)),
                **_open_interest_fields(
                    venue=venue_str,
                    canonical_symbol=canon,
                    venue_symbol=sym,
                    value_quote=(
                        open_interest_quote if has_open_interest_quote else None
                    ),
                    raw_value=raw_open_interest,
                    raw_unit=raw_open_interest_unit,
                    contract_multiplier=1.0 if has_open_interest_quote else None,
                    conversion_mark_price=None,
                    observed_at_ms=(
                        int(_safe_float(raw.get("time", 0)))
                        if has_open_interest_quote and isinstance(raw, dict)
                        else 0
                    ),
                    received_at_ms=market_received_at_ms,
                    source="bybit_tickers",
                    status=oi_status,
                    reason=oi_reason,
                ),
                base_quantity_evidence=metadata_complete,
                price_tick=(
                    _safe_float(price_filter.get("tickSize", 0))
                    if metadata_complete
                    else 0.0
                ),
                quantity_step_base=(
                    _safe_float(lot_size_filter.get("qtyStep", 0))
                    if metadata_complete
                    else 0.0
                ),
                min_quantity_base=(
                    _safe_float(lot_size_filter.get("minOrderQty", 0))
                    if metadata_complete
                    else 0.0
                ),
                min_notional_quote=(
                    _positive_exchange_number(
                        lot_size_filter.get("minNotionalValue")
                    )
                    if metadata_complete
                    else 0.0
                ),
                min_notional_evidence_complete=metadata_complete,
            )
        return result

    # -- Bitget ------------------------------------------------------------

    async def _fetch_bitget_style(self, symbols: list[str]) -> dict[str, FundingTicker]:
        spec = self._spec
        venue_str = spec.venue_id.value
        venue_sym_to_canon: dict[str, str] = {}
        for s in symbols:
            venue_sym_to_canon[self._to_venue_symbol(s)] = s.upper()

        raw, market_received_at_ms = await self._public_get_with_received_at(
            spec.funding_ticker_path,
            params={"productType": "USDT-FUTURES"},
        )
        data = raw.get("data", [])
        items = data if isinstance(data, list) else [data]

        # Bitget documents ticker BBO sizes and order quantities in base coin.
        # The contract-config endpoint supplies symbol identity, quantity step,
        # price step, status, and the funding interval needed to prove that
        # unit contract rather than relying on a venue-wide assumption.
        contract_map: dict[str, dict[str, Any]] = {}
        try:
            contracts_raw = await self._cached_public_get(
                "/api/v2/mix/market/contracts",
                params={"productType": "USDT-FUTURES"},
                max_age_ms=_OKX_CONTRACT_METADATA_MAX_AGE_MS,
            )
        except PublicTransportError:
            contracts_raw = {}
        contracts_data = (
            contracts_raw.get("data", [])
            if isinstance(contracts_raw, dict)
            else []
        )
        for contract in contracts_data if isinstance(contracts_data, list) else []:
            if not isinstance(contract, dict):
                continue
            contract_symbol = str(contract.get("symbol", "") or "")
            if contract_symbol in venue_sym_to_canon:
                contract_map[contract_symbol] = contract

        now_ms = _now_ms()
        funding_map: dict[str, dict[str, Any]] = {}
        funding_received_at_ms = 0
        if spec.funding_rate_path:
            try:
                funding_raw = await self._public_get(
                    spec.funding_rate_path,
                    params={"productType": "USDT-FUTURES"},
                )
            except PublicTransportError:
                funding_raw = {}
            funding_received_at_ms = _now_ms() if funding_raw else 0
            funding_data = (
                funding_raw.get("data", []) if isinstance(funding_raw, dict) else []
            )
            funding_items = funding_data if isinstance(funding_data, list) else [funding_data]
            for funding_item in funding_items:
                if not isinstance(funding_item, dict):
                    continue
                sym = str(funding_item.get("symbol", ""))
                if sym in venue_sym_to_canon:
                    funding_map[sym] = funding_item

        result: dict[str, FundingTicker] = {}
        for item in items:
            sym = str(item.get("symbol", ""))
            canon = venue_sym_to_canon.get(sym)
            if canon is None:
                continue
            mark = _safe_float(
                item.get("markPrice", item.get("lastPr", item.get("last", 0)))
            )
            holding_amount, has_holding_amount, oi_key = _first_present_float(
                item,
                "holdingAmount",
                "openInterest",
            )
            if has_holding_amount and mark > 0.0:
                open_interest_quote = holding_amount * mark
                oi_status = "observed"
                oi_reason = f"{oi_key}_times_mark"
            elif has_holding_amount:
                open_interest_quote = None
                oi_status = "parse_error"
                oi_reason = "missing_mark_price"
            elif oi_key:
                open_interest_quote = None
                oi_status = "parse_error"
                oi_reason = f"invalid_{oi_key}"
            else:
                open_interest_quote = None
                oi_status = "unavailable"
                oi_reason = "missing_open_interest"
            funding_item = funding_map.get(sym, {})
            contract = contract_map.get(sym, {})
            underlying, quote_currency = _canonical_contract_identity(
                self._spec.venue_id,
                canon,
            )
            size_multiplier = _safe_float(contract.get("sizeMultiplier", 0))
            min_trade_quantity = _positive_exchange_number(
                contract.get("minTradeNum")
            )
            price_place = _safe_float(contract.get("pricePlace", -1), default=-1.0)
            price_end_step = _safe_float(contract.get("priceEndStep", 0))
            price_tick = (
                price_end_step * (10.0 ** -int(price_place))
                if price_place >= 0.0
                and float(price_place).is_integer()
                and price_end_step > 0.0
                else 0.0
            )
            metadata_complete = bool(
                str(contract.get("baseCoin", "") or "").strip().upper()
                == underlying
                and str(contract.get("quoteCoin", "") or "").strip().upper()
                == quote_currency
                and str(contract.get("symbolType", "") or "").strip().lower()
                == "perpetual"
                and str(contract.get("symbolStatus", "") or "").strip().lower()
                == "normal"
                and size_multiplier > 0.0
                and min_trade_quantity > 0.0
                and price_tick > 0.0
                and _positive_exchange_number(contract.get("minTradeUSDT")) > 0.0
            )
            fund_interval_hours = _safe_float(contract.get("fundInterval", 0))
            explicit_interval_ms = (
                int(fund_interval_hours * 3_600_000)
                if fund_interval_hours > 0.0
                else 0
            )
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
                market_received_at_ms=market_received_at_ms,
                market_event_at_ms=int(
                    _safe_float(raw.get("requestTime", 0))
                    if isinstance(raw, dict)
                    else 0
                ),
                bid_size=(
                    _safe_float(item.get("bidSz", 0)) if metadata_complete else 0.0
                ),
                ask_size=(
                    _safe_float(item.get("askSz", 0)) if metadata_complete else 0.0
                ),
                mark_price=mark,
                index_price=_safe_float(item.get("indexPrice", 0)),
                funding_rate_bps=_safe_float(
                    funding_item.get("fundingRate", item.get("fundingRate", 0))
                ) * 10000.0,
                **_funding_rate_fields(
                    venue=venue_str,
                    symbol=canon,
                    rate_bps=_safe_float(
                        funding_item.get(
                            "fundingRate", item.get("fundingRate", 0)
                        )
                    )
                    * 10000.0,
                    funding_timestamp_ms=funding_timestamp_ms,
                    observed_at_ms=(
                        funding_received_at_ms
                        if funding_item
                        else market_received_at_ms
                    ),
                    event_at_ms=int(
                        _safe_float(
                            funding_item.get(
                                "requestTime",
                                raw.get("requestTime", 0)
                                if isinstance(raw, dict)
                                else 0,
                            )
                        )
                    ),
                    source=(
                        "bitget_funding_rate"
                        if funding_item
                        else "bitget_market_ticker_funding"
                    ),
                ),
                predicted_funding_rate_bps=_optional_rate_bps(
                    funding_item, "nextFundingRate", "predictedFundingRate"
                ),
                funding_forecast_source=(
                    "venue_predicted_rate"
                    if _optional_rate_bps(
                        funding_item, "nextFundingRate", "predictedFundingRate"
                    ) is not None
                    else "quoted_rate"
                ),
                funding_timestamp_ms=funding_timestamp_ms,
                funding_interval_ms=explicit_interval_ms,
                funding_interval_source=(
                    "bitget_contract_config" if explicit_interval_ms > 0 else ""
                ),
                funding_interval_observed_at_ms=(
                    now_ms if explicit_interval_ms > 0 else 0
                ),
                volume_24h_quote=_safe_float(
                    item.get("usdtVolume", item.get("quoteVolume", 0))
                ),
                **_open_interest_fields(
                    venue=venue_str,
                    canonical_symbol=canon,
                    venue_symbol=sym,
                    value_quote=open_interest_quote,
                    raw_value=(holding_amount if has_holding_amount else None),
                    raw_unit="base" if has_holding_amount else "",
                    contract_multiplier=1.0 if has_holding_amount else None,
                    conversion_mark_price=(mark if mark > 0.0 else None),
                    observed_at_ms=(
                        int(_safe_float(raw.get("requestTime", 0)))
                        if oi_status == "observed" and isinstance(raw, dict)
                        else 0
                    ),
                    received_at_ms=market_received_at_ms,
                    source="bitget_tickers",
                    status=oi_status,
                    reason=oi_reason,
                ),
                base_quantity_evidence=metadata_complete,
                price_tick=price_tick if metadata_complete else 0.0,
                quantity_step_base=(
                    size_multiplier if metadata_complete else 0.0
                ),
                min_quantity_base=(
                    min_trade_quantity if metadata_complete else 0.0
                ),
                min_notional_quote=(
                    _positive_exchange_number(contract.get("minTradeUSDT"))
                    if metadata_complete
                    else 0.0
                ),
                min_notional_evidence_complete=metadata_complete,
            )
        return result

    # -- Gate --------------------------------------------------------------

    async def _fetch_gate_style(self, symbols: list[str]) -> dict[str, FundingTicker]:
        spec = self._spec
        venue_str = spec.venue_id.value
        venue_sym_to_canon: dict[str, str] = {}
        for s in symbols:
            venue_sym_to_canon[self._to_venue_symbol(s)] = s.upper()

        raw, market_received_at_ms = await self._public_get_with_received_at(
            spec.funding_ticker_path
        )
        items = raw if isinstance(raw, list) else [raw]

        now_ms = _now_ms()
        contract_map: dict[str, dict[str, Any]] = {}
        funding_observed_at_ms: dict[str, int] = {}
        missing_contract_symbols: set[str] = set()
        for venue_sym, canon in venue_sym_to_canon.items():
            cache_key = f"{venue_str}:{canon}"
            cached_metadata = self._gate_contract_metadata_by_key.get(cache_key)
            if cached_metadata is not None:
                metadata, metadata_observed_at_ms = cached_metadata
                metadata_age_ms = now_ms - int(metadata_observed_at_ms)
                if 0 <= metadata_age_ms <= _GATE_CONTRACT_METADATA_MAX_AGE_MS:
                    contract_map[venue_sym] = dict(metadata)
                else:
                    self._gate_contract_metadata_by_key.pop(cache_key, None)
            if self._funding_rate_is_fresh(cache_key, now_ms):
                rate_bps, ts_ms, cached_observed_at_ms = self._funding_cache[cache_key]
                funding_observed_at_ms[venue_sym] = int(cached_observed_at_ms)
                contract_map.setdefault(venue_sym, {}).update(
                    {
                        "funding_rate": str(rate_bps / 10000.0),
                        "funding_next_apply": ts_ms,
                    }
                )
            if (
                not self._funding_rate_is_fresh(cache_key, now_ms)
                or venue_sym not in contract_map
                or not _gate_contract_metadata_complete(contract_map[venue_sym])
            ):
                missing_contract_symbols.add(venue_sym)

        if spec.funding_contracts_path and missing_contract_symbols:
            try:
                contracts_raw = await self._public_get(spec.funding_contracts_path)
            except PublicTransportError:
                contracts_raw = []
            contract_items = (
                contracts_raw if isinstance(contracts_raw, list) else [contracts_raw]
            )
            for contract_item in contract_items:
                if not isinstance(contract_item, dict):
                    continue
                sym = str(
                    contract_item.get(
                        "name",
                        contract_item.get("contract", ""),
                    )
                )
                if not sym:
                    continue
                canon = venue_sym_to_canon.get(
                    sym,
                    self._from_venue_symbol(sym).upper(),
                )
                cache_key = f"{venue_str}:{canon}"
                self._gate_contract_metadata_by_key[cache_key] = (
                    dict(contract_item),
                    now_ms,
                )
                funding_timestamp_ms = _funding_timestamp_ms_or_seconds(
                    contract_item.get("funding_next_apply", 0)
                )
                funding_is_fresh = funding_timestamp_ms > now_ms + _FUNDING_CACHE_MIN_FUTURE_MS
                if funding_is_fresh and _has_nonempty_field(contract_item, "funding_rate"):
                    self._funding_cache[f"{venue_str}:{canon}"] = (
                        _safe_float(contract_item.get("funding_rate", 0)) * 10000.0,
                        funding_timestamp_ms,
                        now_ms,
                    )
                    if sym in venue_sym_to_canon:
                        funding_observed_at_ms[sym] = now_ms
                if sym in venue_sym_to_canon and (funding_is_fresh or sym not in contract_map):
                    contract_map[sym] = contract_item

        result: dict[str, FundingTicker] = {}
        for item in items:
            sym = str(item.get("contract", ""))
            canon = venue_sym_to_canon.get(sym)
            if canon is None:
                continue
            mark = _safe_float(item.get("mark_price", 0))
            contract_item = contract_map.get(sym, {})
            # Gate ticker sizes are contract counts.  A missing symbol-level
            # quanto multiplier is unknown, never an implicit 1x contract.
            quanto = _positive_exchange_number(
                contract_item.get("quanto_multiplier")
            )
            oi_contracts, has_oi_contracts, oi_key = _first_present_float(
                item,
                "total_size",
            )
            if has_oi_contracts and quanto > 0 and mark > 0:
                open_interest_quote = oi_contracts * quanto * mark
                oi_status = "observed"
                oi_reason = f"{oi_key}_times_quanto_mark"
            elif has_oi_contracts:
                open_interest_quote = None
                oi_status = "parse_error"
                oi_reason = (
                    "missing_contract_multiplier"
                    if quanto <= 0
                    else "missing_mark_price"
                )
            elif oi_key:
                open_interest_quote = None
                oi_status = "parse_error"
                oi_reason = f"invalid_{oi_key}"
            else:
                open_interest_quote = None
                oi_status = "unavailable"
                oi_reason = "missing_open_interest"
            funding_timestamp_ms = _funding_timestamp_ms_or_seconds(
                contract_item.get("funding_next_apply", 0)
            )
            if funding_timestamp_ms <= now_ms + _FUNDING_CACHE_MIN_FUTURE_MS:
                funding_timestamp_ms = 0
            bid_size_contracts = _safe_float(
                item.get("highest_size", item.get("bid_size", 0))
            )
            ask_size_contracts = _safe_float(
                item.get("lowest_size", item.get("ask_size", 0))
            )
            size_multiplier = quanto if quanto > 0 else 0.0
            order_size_min_contracts = _positive_exchange_number(
                contract_item.get("order_size_min")
            )
            order_price_round = _positive_exchange_number(
                contract_item.get("order_price_round")
            )
            metadata_complete = _gate_contract_metadata_complete(contract_item)
            funding_interval_seconds = _safe_float(
                contract_item.get("funding_interval", 0)
            )
            explicit_interval_ms = (
                int(funding_interval_seconds * 1_000)
                if funding_interval_seconds > 0
                else 0
            )
            result[f"{venue_str}:{canon}"] = FundingTicker(
                venue=venue_str,
                symbol=canon,
                bid=_safe_float(item.get("highest_bid", 0)),
                ask=_safe_float(item.get("lowest_ask", 0)),
                market_received_at_ms=market_received_at_ms,
                bid_size=bid_size_contracts * size_multiplier,
                ask_size=ask_size_contracts * size_multiplier,
                mark_price=mark,
                index_price=_safe_float(item.get("index_price", 0)),
                funding_rate_bps=_safe_float(
                    contract_item.get("funding_rate", item.get("funding_rate", 0))
                ) * 10000.0,
                **_funding_rate_fields(
                    venue=venue_str,
                    symbol=canon,
                    rate_bps=_safe_float(
                        contract_item.get(
                            "funding_rate", item.get("funding_rate", 0)
                        )
                    )
                    * 10000.0,
                    funding_timestamp_ms=funding_timestamp_ms,
                    observed_at_ms=funding_observed_at_ms.get(
                        sym,
                        market_received_at_ms
                        if _has_nonempty_field(item, "funding_rate")
                        else 0,
                    ),
                    source=(
                        "gate_contract_funding"
                        if sym in funding_observed_at_ms
                        else "gate_market_ticker_funding"
                    ),
                ),
                predicted_funding_rate_bps=_optional_rate_bps(
                    contract_item, "funding_rate_indicative", "next_funding_rate"
                ),
                funding_forecast_source=(
                    "venue_indicative_rate"
                    if _optional_rate_bps(
                        contract_item, "funding_rate_indicative", "next_funding_rate"
                    ) is not None
                    else "quoted_rate"
                ),
                funding_timestamp_ms=funding_timestamp_ms,
                funding_interval_ms=explicit_interval_ms,
                funding_interval_source=(
                    "gate_contract_metadata" if explicit_interval_ms > 0 else ""
                ),
                funding_interval_observed_at_ms=(
                    now_ms if explicit_interval_ms > 0 else 0
                ),
                volume_24h_quote=_safe_float(
                    item.get("volume_24h_quote", item.get("volume_24h", 0))
                ),
                **_open_interest_fields(
                    venue=venue_str,
                    canonical_symbol=canon,
                    venue_symbol=sym,
                    value_quote=open_interest_quote,
                    raw_value=(oi_contracts if has_oi_contracts else None),
                    raw_unit="contracts" if has_oi_contracts else "",
                    contract_multiplier=(quanto if quanto > 0.0 else None),
                    conversion_mark_price=(mark if mark > 0.0 else None),
                    # Gate's bulk ticker does not carry a response-wide event
                    # timestamp.  Unknown vendor time is 0; local freshness is
                    # anchored to the actual HTTP receipt below.
                    observed_at_ms=0,
                    received_at_ms=market_received_at_ms,
                    source="gate_tickers",
                    status=oi_status,
                    reason=oi_reason,
                ),
                base_quantity_evidence=metadata_complete,
                price_tick=(order_price_round if metadata_complete else 0.0),
                quantity_step_base=(
                    order_size_min_contracts * quanto
                    if metadata_complete
                    else 0.0
                ),
                min_quantity_base=(
                    order_size_min_contracts * quanto
                    if metadata_complete
                    else 0.0
                ),
                min_notional_quote=(
                    0.0
                ),
                min_notional_evidence_complete=metadata_complete,
            )
        return result

    # -- Hyperliquid -------------------------------------------------------

    async def _fetch_hyperliquid_style(self, symbols: list[str]) -> dict[str, FundingTicker]:
        spec = self._spec
        venue_str = spec.venue_id.value
        canonical_set = {s.upper() for s in symbols}
        # Use metaAndAssetCtxs as a bulk call (plan requirement)
        body = {"type": "metaAndAssetCtxs"}
        raw, market_received_at_ms = await self._public_post_with_received_at(
            spec.funding_ticker_path,
            body=body,
        )

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
        funding_ts = _next_hour_boundary(market_received_at_ms)

        result: dict[str, FundingTicker] = {}
        for idx, item in enumerate(universe):
            if not isinstance(item, dict):
                continue
            size_decimals = _hyperliquid_size_decimals(item)
            if size_decimals is None:
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
            price_tick = _hyperliquid_price_tick(mid_price, size_decimals)
            quantity_step = 10.0 ** (-size_decimals)
            metadata_complete = price_tick > 0.0 and quantity_step > 0.0

            open_interest, has_open_interest, oi_key = _first_present_float(
                ctx,
                "openInterest",
            )
            if has_open_interest and mark > 0.0:
                open_interest_quote = open_interest * mark
                oi_status = "observed"
                oi_reason = f"{oi_key}_times_mark"
            elif has_open_interest:
                open_interest_quote = None
                oi_status = "parse_error"
                oi_reason = "missing_mark_price"
            elif oi_key:
                open_interest_quote = None
                oi_status = "parse_error"
                oi_reason = f"invalid_{oi_key}"
            else:
                open_interest_quote = None
                oi_status = "unavailable"
                oi_reason = "missing_open_interest"

            result[f"{venue_str}:{canon.upper()}"] = FundingTicker(
                venue=venue_str,
                symbol=canon.upper(),
                bid=best_bid,
                ask=best_ask,
                market_received_at_ms=market_received_at_ms,
                bid_size=bid_size if metadata_complete else 0.0,
                ask_size=ask_size if metadata_complete else 0.0,
                mark_price=mark,
                index_price=_safe_float(
                    ctx.get("oraclePx", ctx.get("indexPx", item.get("indexPx", 0)))
                ),
                funding_rate_bps=_safe_float(ctx.get("funding", 0)) * 10000.0,
                **_funding_rate_fields(
                    venue=venue_str,
                    symbol=canon.upper(),
                    rate_bps=_safe_float(ctx.get("funding", 0)) * 10000.0,
                    funding_timestamp_ms=funding_ts,
                    observed_at_ms=market_received_at_ms,
                    source="hyperliquid_meta_and_asset_contexts",
                ),
                funding_timestamp_ms=funding_ts,
                funding_interval_ms=3_600_000,
                funding_interval_source="hyperliquid_protocol_hourly",
                funding_interval_observed_at_ms=market_received_at_ms,
                funding_forecast_source="quoted_rate_hourly_protocol",
                volume_24h_quote=_safe_float(ctx.get("dayNtlVlm", 0)),
                **_open_interest_fields(
                    venue=venue_str,
                    canonical_symbol=canon.upper(),
                    venue_symbol=name,
                    value_quote=open_interest_quote,
                    raw_value=(open_interest if has_open_interest else None),
                    raw_unit="base" if has_open_interest else "",
                    contract_multiplier=1.0 if has_open_interest else None,
                    conversion_mark_price=(mark if mark > 0.0 else None),
                    # metaAndAssetCtxs has no payload event timestamp.  Do not
                    # forge one from the request-start wall clock.
                    observed_at_ms=0,
                    received_at_ms=market_received_at_ms,
                    source="hyperliquid_meta_and_asset_ctxs",
                    status=oi_status,
                    reason=oi_reason,
                ),
                base_quantity_evidence=metadata_complete,
                price_tick=price_tick if metadata_complete else 0.0,
                quantity_step_base=(
                    quantity_step if metadata_complete else 0.0
                ),
                min_quantity_base=(
                    quantity_step if metadata_complete else 0.0
                ),
                min_notional_quote=(
                    10.0 if metadata_complete else 0.0
                ),
                min_notional_evidence_complete=metadata_complete,
            )
        return result

# ---------------------------------------------------------------------------
# Transport error (lightweight, no dependency on the full transport module)
# ---------------------------------------------------------------------------


class PublicTransportErrorCategory:
    TRANSPORT_FAILURE = "transport_failure"
    UNSUPPORTED_CAPABILITY = "unsupported_capability"
    PARSE_FAILURE = "parse_failure"


class PublicTransportError(Exception):
    def __init__(
        self,
        category: str,
        message: str,
        status_code: int = 0,
        phase_timings: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.category = category
        self.status_code = status_code
        self.phase_timings = dict(phase_timings or {})


def _parse_retry_after_ms(headers: dict[str, str]) -> Optional[int]:
    retry_after = headers.get("Retry-After", headers.get("retry-after", ""))
    if not retry_after:
        return None
    try:
        return int(retry_after) * 1000
    except ValueError:
        return None
