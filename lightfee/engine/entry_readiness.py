"""Entry readiness provider boundary for final candidate selection."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Protocol

from lightfee.config.schema import ENTRY_READINESS_PROVIDERS
from lightfee.marketdata.l2 import L2BookStatus, LocalL2BookKey


@dataclass(frozen=True)
class EntryReadinessDecision:
    """Provider decision for one entry candidate."""

    allowed: bool
    reason: str = ""
    symbol: str = ""
    pair_id: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def allow(
        cls,
        *,
        symbol: str = "",
        pair_id: str = "",
        evidence: dict[str, Any] | None = None,
    ) -> "EntryReadinessDecision":
        return cls(
            allowed=True,
            symbol=symbol,
            pair_id=pair_id,
            evidence=dict(evidence or {}),
        )

    @classmethod
    def block(
        cls,
        reason: str,
        *,
        symbol: str = "",
        pair_id: str = "",
        evidence: dict[str, Any] | None = None,
    ) -> "EntryReadinessDecision":
        return cls(
            allowed=False,
            reason=str(reason),
            symbol=symbol,
            pair_id=pair_id,
            evidence=dict(evidence or {}),
        )


class EntryReadinessProvider(Protocol):
    """Decides whether a final entry candidate has enough quote evidence."""

    def decide(
        self,
        candidate: Any,
        now_ms: int,
        *,
        market_quotes: Any = None,
    ) -> EntryReadinessDecision:
        ...


class LocalL2EntryReadinessProvider:
    """Default provider preserving the existing local-L2 gate semantics."""

    def __init__(self, runtime: Any) -> None:
        self._runtime = runtime

    def decide(
        self,
        candidate: Any,
        now_ms: int,
        *,
        market_quotes: Any = None,
    ) -> EntryReadinessDecision:
        symbol = str(getattr(candidate, "symbol", ""))
        pair_id = self._runtime._candidate_pair_id(candidate)
        reason = self._runtime._entry_local_l2_selection_blocker(candidate, now_ms)
        if reason:
            return EntryReadinessDecision.block(
                str(reason),
                symbol=symbol,
                pair_id=pair_id,
                evidence={"provider": "local_l2"},
            )
        return EntryReadinessDecision.allow(
            symbol=symbol,
            pair_id=pair_id,
            evidence={"provider": "local_l2"},
        )


@dataclass(frozen=True)
class QuoteLease:
    """Time-bound top-of-book evidence for one candidate pair."""

    pair_id: str
    symbol: str
    long_venue: str
    short_venue: str
    long_bid: float
    long_ask: float
    short_bid: float
    short_ask: float
    long_observed_at_ms: int
    short_observed_at_ms: int
    created_at_ms: int
    expires_at_ms: int
    provider: str = "quote_lease"


class RestTopBookEntryReadinessProvider:
    """Readiness provider backed by fresh sidecar/REST top-of-book quotes."""

    provider_name = "rest_top_book"
    reason_prefix = "entry_rest_top_book"

    def __init__(self, runtime: Any) -> None:
        self._runtime = runtime

    def decide(
        self,
        candidate: Any,
        now_ms: int,
        *,
        market_quotes: Any = None,
    ) -> EntryReadinessDecision:
        validation = self._validate_quotes(candidate, now_ms, market_quotes)
        if isinstance(validation, EntryReadinessDecision):
            return validation
        symbol, pair_id, long_quote, short_quote = validation
        evidence = self._quote_evidence(
            symbol,
            pair_id,
            long_quote,
            short_quote,
            now_ms,
        )
        return EntryReadinessDecision.allow(
            symbol=symbol,
            pair_id=pair_id,
            evidence=evidence,
        )

    def _reason(self, suffix: str) -> str:
        return f"{self.reason_prefix}_{suffix}"

    def _validate_quotes(
        self,
        candidate: Any,
        now_ms: int,
        market_quotes: Any,
    ) -> EntryReadinessDecision | tuple[str, str, Any, Any]:
        if self._runtime.config.runtime.mode != "live":
            symbol = str(getattr(candidate, "symbol", ""))
            pair_id = self._runtime._candidate_pair_id(candidate)
            return EntryReadinessDecision.allow(
                symbol=symbol,
                pair_id=pair_id,
                evidence={"provider": self.provider_name, "mode": "paper"},
            )

        symbol = str(getattr(candidate, "symbol", ""))
        pair_id = self._runtime._candidate_pair_id(candidate)
        long_venue = str(getattr(candidate, "long_venue", ""))
        short_venue = str(getattr(candidate, "short_venue", ""))
        quote_lookup = self._runtime._market_quote_lookup(market_quotes)
        long_quote = self._runtime._candidate_quote(quote_lookup, long_venue, symbol)
        short_quote = self._runtime._candidate_quote(quote_lookup, short_venue, symbol)
        if long_quote is None or short_quote is None:
            return EntryReadinessDecision.block(
                self._reason("missing_quote"),
                symbol=symbol,
                pair_id=pair_id,
                evidence={
                    "provider": self.provider_name,
                    "missing_long_quote": long_quote is None,
                    "missing_short_quote": short_quote is None,
                },
            )

        quote_error = (
            self._quote_error(long_quote, "ask", now_ms)
            or self._quote_error(short_quote, "bid", now_ms)
        )
        if quote_error:
            reason, evidence = quote_error
            evidence.update({"provider": self.provider_name})
            return EntryReadinessDecision.block(
                reason,
                symbol=symbol,
                pair_id=pair_id,
                evidence=evidence,
            )
        return symbol, pair_id, long_quote, short_quote

    def _quote_error(
        self,
        quote: Any,
        executable_side: str,
        now_ms: int,
    ) -> tuple[str, dict[str, Any]] | None:
        bid = float(getattr(quote, "bid", 0.0) or 0.0)
        ask = float(getattr(quote, "ask", 0.0) or 0.0)
        if bid <= 0.0 or ask <= 0.0:
            evidence = self._quote_base_evidence(quote, now_ms)
            evidence["blocker_family"] = "invalid_quote"
            return self._reason("invalid_quote"), evidence
        if bid >= ask:
            evidence = self._quote_base_evidence(quote, now_ms)
            evidence["blocker_family"] = "invalid_quote"
            return self._reason("crossed_quote"), evidence
        price = ask if executable_side == "ask" else bid
        if price <= 0.0:
            evidence = self._quote_base_evidence(quote, now_ms)
            evidence["blocker_family"] = "invalid_quote"
            return self._reason("invalid_quote"), evidence
        observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
        max_age_ms = int(getattr(self._runtime.config.runtime, "max_market_age_ms", 0) or 0)
        if observed_at_ms <= 0 or max_age_ms <= 0:
            evidence = self._quote_base_evidence(quote, now_ms)
            evidence["blocker_family"] = "stale_quote"
            return self._reason("stale_quote"), evidence
        age_ms = max(now_ms - observed_at_ms, 0)
        if age_ms > max_age_ms:
            evidence = self._quote_base_evidence(quote, now_ms)
            evidence["max_age_ms"] = max_age_ms
            evidence["blocker_family"] = "stale_quote"
            return self._reason("stale_quote"), evidence
        return None

    @staticmethod
    def _quote_base_evidence(quote: Any, now_ms: int) -> dict[str, Any]:
        observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
        return {
            "venue": str(getattr(quote, "venue", "")),
            "symbol": str(getattr(quote, "symbol", "")),
            "bid": float(getattr(quote, "bid", 0.0) or 0.0),
            "ask": float(getattr(quote, "ask", 0.0) or 0.0),
            "observed_at_ms": observed_at_ms,
            "age_ms": max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else None,
        }

    def _quote_evidence(
        self,
        symbol: str,
        pair_id: str,
        long_quote: Any,
        short_quote: Any,
        now_ms: int,
    ) -> dict[str, Any]:
        return {
            "provider": self.provider_name,
            "symbol": symbol,
            "pair_id": pair_id,
            "long_quote": self._quote_base_evidence(long_quote, now_ms),
            "short_quote": self._quote_base_evidence(short_quote, now_ms),
        }


class QuoteLeaseEntryReadinessProvider(RestTopBookEntryReadinessProvider):
    """Top-of-book readiness provider that records a short-lived quote lease."""

    provider_name = "quote_lease"
    reason_prefix = "entry_quote_lease"

    def __init__(self, runtime: Any) -> None:
        super().__init__(runtime)
        self._leases: dict[str, QuoteLease] = {}

    def decide(
        self,
        candidate: Any,
        now_ms: int,
        *,
        market_quotes: Any = None,
    ) -> EntryReadinessDecision:
        validation = self._validate_quotes(candidate, now_ms, market_quotes)
        if isinstance(validation, EntryReadinessDecision):
            return validation
        symbol, pair_id, long_quote, short_quote = validation
        lease = self._make_lease(candidate, symbol, pair_id, long_quote, short_quote, now_ms)
        self._leases[pair_id] = lease
        evidence = self._quote_evidence(symbol, pair_id, long_quote, short_quote, now_ms)
        evidence["lease"] = {
            "created_at_ms": lease.created_at_ms,
            "expires_at_ms": lease.expires_at_ms,
        }
        return EntryReadinessDecision.allow(
            symbol=symbol,
            pair_id=pair_id,
            evidence=evidence,
        )

    def get_lease(self, pair_id: str) -> QuoteLease | None:
        return self._leases.get(str(pair_id))

    def _make_lease(
        self,
        candidate: Any,
        symbol: str,
        pair_id: str,
        long_quote: Any,
        short_quote: Any,
        now_ms: int,
    ) -> QuoteLease:
        ttl_ms = int(
            getattr(self._runtime.config.strategy, "entry_quote_lease_ttl_ms", 0) or 0
        )
        return QuoteLease(
            pair_id=pair_id,
            symbol=symbol,
            long_venue=str(getattr(candidate, "long_venue", "")),
            short_venue=str(getattr(candidate, "short_venue", "")),
            long_bid=float(getattr(long_quote, "bid", 0.0) or 0.0),
            long_ask=float(getattr(long_quote, "ask", 0.0) or 0.0),
            short_bid=float(getattr(short_quote, "bid", 0.0) or 0.0),
            short_ask=float(getattr(short_quote, "ask", 0.0) or 0.0),
            long_observed_at_ms=int(getattr(long_quote, "observed_at_ms", 0) or 0),
            short_observed_at_ms=int(getattr(short_quote, "observed_at_ms", 0) or 0),
            created_at_ms=now_ms,
            expires_at_ms=now_ms + ttl_ms,
            provider=self.provider_name,
        )


class WsTopBookEntryReadinessProvider(QuoteLeaseEntryReadinessProvider):
    """Readiness provider backed by fresh WebSocket best bid/ask evidence."""

    provider_name = "ws_top_book"
    reason_prefix = "entry_ws_top_book"

    def _validate_quotes(
        self,
        candidate: Any,
        now_ms: int,
        market_quotes: Any,
    ) -> EntryReadinessDecision | tuple[str, str, Any, Any]:
        if self._runtime.config.runtime.mode != "live":
            symbol = str(getattr(candidate, "symbol", ""))
            pair_id = self._runtime._candidate_pair_id(candidate)
            return EntryReadinessDecision.allow(
                symbol=symbol,
                pair_id=pair_id,
                evidence={"provider": self.provider_name, "mode": "paper"},
            )

        symbol = str(getattr(candidate, "symbol", ""))
        pair_id = self._runtime._candidate_pair_id(candidate)
        long_venue = str(getattr(candidate, "long_venue", ""))
        short_venue = str(getattr(candidate, "short_venue", ""))
        long_book = self._runtime.local_l2_runtime.get_book(long_venue, symbol)
        short_book = self._runtime.local_l2_runtime.get_book(short_venue, symbol)
        if long_book is None or short_book is None:
            return EntryReadinessDecision.block(
                self._reason("missing_book"),
                symbol=symbol,
                pair_id=pair_id,
                evidence={
                    "provider": self.provider_name,
                    "missing_long_book": long_book is None,
                    "missing_short_book": short_book is None,
                },
            )

        long_error = self._book_error(long_book, now_ms)
        short_error = self._book_error(short_book, now_ms)
        if long_error or short_error:
            reason, evidence = long_error or short_error
            evidence.update({"provider": self.provider_name})
            return EntryReadinessDecision.block(
                reason,
                symbol=symbol,
                pair_id=pair_id,
                evidence=evidence,
            )

        return (
            symbol,
            pair_id,
            self._quote_from_book(long_book),
            self._quote_from_book(short_book),
        )

    def _book_error(
        self,
        book: Any,
        now_ms: int,
    ) -> tuple[str, dict[str, Any]] | None:
        if getattr(book, "status", None) != L2BookStatus.HOT:
            return self._reason("book_not_hot"), self._book_evidence(book, now_ms)
        bid = float(book.best_bid() if hasattr(book, "best_bid") else 0.0)
        ask = float(book.best_ask() if hasattr(book, "best_ask") else 0.0)
        if bid <= 0.0 or ask <= 0.0:
            return self._reason("invalid_book"), self._book_evidence(book, now_ms)
        if bid >= ask:
            return self._reason("crossed_book"), self._book_evidence(book, now_ms)

        max_age_ms = int(getattr(self._runtime.config.runtime, "max_market_age_ms", 0) or 0)
        observed_at_ms = int(getattr(book, "observed_at_ms", 0) or 0)
        if observed_at_ms <= 0 or max_age_ms <= 0 or (now_ms - observed_at_ms) > max_age_ms:
            evidence = self._book_evidence(book, now_ms)
            evidence["max_age_ms"] = max_age_ms
            return self._reason("stale_book"), evidence

        ws_evidence_ms = self._ws_evidence_ms(str(book.venue), str(book.symbol))
        if ws_evidence_ms <= 0:
            return self._reason("missing_ws_evidence"), self._book_evidence(book, now_ms)
        if (now_ms - ws_evidence_ms) > max_age_ms:
            evidence = self._book_evidence(book, now_ms)
            evidence["ws_evidence_at_ms"] = ws_evidence_ms
            evidence["max_age_ms"] = max_age_ms
            return self._reason("stale_ws_evidence"), evidence
        return None

    def _ws_evidence_ms(self, venue: str, symbol: str) -> int:
        data_plane = getattr(self._runtime, "l2_data_plane", None)
        states = getattr(data_plane, "_freshness_states", {}) or {}
        state = states.get(LocalL2BookKey(venue=venue, symbol=symbol))
        if state is None:
            return 0
        return max(
            int(getattr(state, "last_ws_delta_ms", 0) or 0),
            int(getattr(state, "last_book_confirmation_ms", 0) or 0),
        )

    @staticmethod
    def _quote_from_book(book: Any) -> Any:
        return SimpleNamespace(
            venue=str(getattr(book, "venue", "")),
            symbol=str(getattr(book, "symbol", "")),
            bid=float(book.best_bid() if hasattr(book, "best_bid") else 0.0),
            ask=float(book.best_ask() if hasattr(book, "best_ask") else 0.0),
            observed_at_ms=int(getattr(book, "observed_at_ms", 0) or 0),
        )

    @staticmethod
    def _book_evidence(book: Any, now_ms: int) -> dict[str, Any]:
        observed_at_ms = int(getattr(book, "observed_at_ms", 0) or 0)
        status = getattr(book, "status", "")
        return {
            "venue": str(getattr(book, "venue", "")),
            "symbol": str(getattr(book, "symbol", "")),
            "status": status.value if hasattr(status, "value") else str(status),
            "bid": float(book.best_bid() if hasattr(book, "best_bid") else 0.0),
            "ask": float(book.best_ask() if hasattr(book, "best_ask") else 0.0),
            "observed_at_ms": observed_at_ms,
            "age_ms": max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else None,
        }


class WsBboQuoteLeaseEntryReadinessProvider(QuoteLeaseEntryReadinessProvider):
    """Readiness provider backed by independent per-venue WS BBO quotes."""

    provider_name = "ws_bbo_quote_lease"
    reason_prefix = "entry_ws_bbo_quote_lease"

    def _validate_quotes(
        self,
        candidate: Any,
        now_ms: int,
        market_quotes: Any,
    ) -> EntryReadinessDecision | tuple[str, str, Any, Any]:
        if self._runtime.config.runtime.mode != "live":
            symbol = str(getattr(candidate, "symbol", ""))
            pair_id = self._runtime._candidate_pair_id(candidate)
            return EntryReadinessDecision.allow(
                symbol=symbol,
                pair_id=pair_id,
                evidence={"provider": self.provider_name, "mode": "paper"},
            )

        symbol = str(getattr(candidate, "symbol", ""))
        pair_id = self._runtime._candidate_pair_id(candidate)
        long_venue = str(getattr(candidate, "long_venue", ""))
        short_venue = str(getattr(candidate, "short_venue", ""))
        cache = getattr(self._runtime, "ws_bbo_cache", None)
        long_quote = cache.get_quote(long_venue, symbol) if cache is not None else None
        short_quote = cache.get_quote(short_venue, symbol) if cache is not None else None
        long_stream_state = self._stream_state(long_venue, symbol)
        short_stream_state = self._stream_state(short_venue, symbol)
        long_quote, long_rest_refresh = self._quote_with_rest_refresh(
            long_venue,
            symbol,
            long_quote,
            "ask",
            now_ms,
            long_stream_state,
        )
        short_quote, short_rest_refresh = self._quote_with_rest_refresh(
            short_venue,
            symbol,
            short_quote,
            "bid",
            now_ms,
            short_stream_state,
        )
        rest_refresh_evidence = {}
        if long_rest_refresh is not None:
            rest_refresh_evidence["long"] = long_rest_refresh
        if short_rest_refresh is not None:
            rest_refresh_evidence["short"] = short_rest_refresh
        if long_quote is None or short_quote is None:
            evidence = {
                "provider": self.provider_name,
                "blocker_family": "waiting_for_subscription",
                "missing_long_quote": long_quote is None,
                "missing_short_quote": short_quote is None,
                "source": "ws_bbo_cache",
                "long_stream_state": long_stream_state,
                "short_stream_state": short_stream_state,
            }
            if rest_refresh_evidence:
                evidence["rest_refresh"] = rest_refresh_evidence
            return EntryReadinessDecision.block(
                self._reason("missing_quote"),
                symbol=symbol,
                pair_id=pair_id,
                evidence=evidence,
            )

        long_quote_error = self._quote_error(long_quote, "ask", now_ms)
        short_quote_error = self._quote_error(short_quote, "bid", now_ms)
        quote_error = long_quote_error or short_quote_error
        if quote_error:
            reason, evidence = quote_error
            evidence.update({"provider": self.provider_name, "source": "ws_bbo_cache"})
            evidence.setdefault(
                "quote_age_ms",
                {
                    "long": self._quote_base_evidence(long_quote, now_ms)["age_ms"],
                    "short": self._quote_base_evidence(short_quote, now_ms)["age_ms"],
                },
            )
            if rest_refresh_evidence:
                evidence["rest_refresh"] = rest_refresh_evidence
            return EntryReadinessDecision.block(
                reason,
                symbol=symbol,
                pair_id=pair_id,
                evidence=evidence,
            )
        return symbol, pair_id, long_quote, short_quote

    def _quote_error(
        self,
        quote: Any,
        executable_side: str,
        now_ms: int,
    ) -> tuple[str, dict[str, Any]] | None:
        base_error = super()._quote_error(quote, executable_side, now_ms)
        if base_error is not None:
            return base_error

        max_age_ms = self._quote_lease_age_budget_ms()
        observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
        age_ms = max(now_ms - observed_at_ms, 0) if observed_at_ms > 0 else None
        if (
            observed_at_ms <= 0
            or max_age_ms <= 0
            or age_ms is None
            or age_ms > max_age_ms
        ):
            evidence = self._quote_base_evidence(quote, now_ms)
            evidence["max_age_ms"] = max_age_ms
            evidence["age_budget_source"] = "entry_quote_lease_ttl_ms"
            evidence["blocker_family"] = "stale_quote"
            return self._reason("stale_quote"), evidence
        return None

    def _quote_with_rest_refresh(
        self,
        venue: str,
        symbol: str,
        quote: Any,
        executable_side: str,
        now_ms: int,
        stream_state: dict[str, Any],
    ) -> tuple[Any, dict[str, Any] | None]:
        venue_key = str(venue or "").strip().lower()
        symbol_key = str(symbol or "").strip().upper()
        if not bool(stream_state.get("tracked")):
            return quote, None
        if not self._quote_needs_rest_refresh(quote, executable_side, now_ms):
            return quote, None

        evidence: dict[str, Any] = {
            "attempted": True,
            "source": "rest_top_book_refresh",
            "venue": venue_key,
            "symbol": symbol_key,
            "quote_present_before_refresh": quote is not None,
        }

        refresher = self._rest_top_book_refresher()
        refresh_quote = getattr(refresher, "refresh_quote", None)
        if not callable(refresh_quote):
            evidence["outcome"] = "no_refresher"
            return quote, evidence
        try:
            refreshed = refresh_quote(
                venue_key,
                symbol_key,
                now_ms=now_ms,
            )
        except Exception as exc:  # pragma: no cover - defensive telemetry
            evidence["outcome"] = "error"
            evidence["error"] = f"{type(exc).__name__}: {exc}"[:240]
            return quote, evidence
        if refreshed is None:
            evidence["outcome"] = "no_quote"
            return quote, evidence
        refreshed_error = self._quote_error(refreshed, executable_side, now_ms)
        if refreshed_error:
            reason, error_evidence = refreshed_error
            evidence["outcome"] = "invalid_quote"
            evidence["reason"] = reason
            evidence["quote_evidence"] = error_evidence
            return quote, evidence

        cache = getattr(self._runtime, "ws_bbo_cache", None)
        if cache is not None and hasattr(cache, "update_quote"):
            if cache.update_quote(refreshed):
                evidence["outcome"] = "cache_updated"
                evidence["observed_at_ms"] = int(
                    getattr(refreshed, "observed_at_ms", 0) or 0
                )
                return cache.get_quote(venue, symbol) or refreshed, evidence
            evidence["outcome"] = "cache_rejected"
            evidence["quote_evidence"] = self._quote_base_evidence(
                refreshed,
                now_ms,
            )
            return quote, evidence
        evidence["outcome"] = "refreshed"
        evidence["observed_at_ms"] = int(getattr(refreshed, "observed_at_ms", 0) or 0)
        return refreshed, evidence

    def _quote_needs_rest_refresh(
        self,
        quote: Any,
        executable_side: str,
        now_ms: int,
    ) -> bool:
        if quote is None:
            return True
        quote_error = self._quote_error(quote, executable_side, now_ms)
        if quote_error is None:
            return False
        reason, _ = quote_error
        return reason == self._reason("stale_quote")

    def _quote_lease_age_budget_ms(self) -> int:
        return int(
            getattr(self._runtime.config.strategy, "entry_quote_lease_ttl_ms", 0) or 0
        )

    def _rest_top_book_refresher(self) -> Any:
        refresher = getattr(self._runtime, "ws_bbo_rest_refresher", None)
        if refresher is not None:
            return refresher
        from lightfee.marketdata.ws_bbo import RestTopBookQuoteRefresher

        refresher = RestTopBookQuoteRefresher(timeout_ms=750)
        setattr(self._runtime, "ws_bbo_rest_refresher", refresher)
        return refresher

    def _stream_state(self, venue: str, symbol: str) -> dict[str, Any]:
        data_plane = getattr(self._runtime, "ws_bbo_data_plane", None)
        if data_plane is None or not hasattr(data_plane, "stream_state"):
            return {
                "venue": str(venue),
                "symbol": str(symbol).upper(),
                "tracked": False,
            }
        return data_plane.stream_state(venue, symbol)


def build_entry_readiness_provider(runtime: Any) -> EntryReadinessProvider:
    provider = str(
        getattr(runtime.config.strategy, "entry_readiness_provider", "local_l2") or "local_l2"
    ).strip().lower()
    if provider == "local_l2":
        return LocalL2EntryReadinessProvider(runtime)
    if provider == "rest_top_book":
        return RestTopBookEntryReadinessProvider(runtime)
    if provider == "quote_lease":
        return QuoteLeaseEntryReadinessProvider(runtime)
    if provider == "ws_top_book":
        return WsTopBookEntryReadinessProvider(runtime)
    if provider == "ws_bbo_quote_lease":
        return WsBboQuoteLeaseEntryReadinessProvider(runtime)
    raise ValueError(
        "unknown entry_readiness_provider "
        f"{provider!r}; expected one of {list(ENTRY_READINESS_PROVIDERS)}"
    )
