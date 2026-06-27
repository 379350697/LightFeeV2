"""Venue symbol eligibility helpers for pre-HTTP private/public probes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from lightfee.core.domain import Venue


SUPPORTED = "supported"
UNSUPPORTED_BEFORE_HTTP = "unsupported_before_http"
TRUTH_UNAVAILABLE = "truth_unavailable"
PRIVATE_TRUTH_UNSUPPORTED_REASON = "symbol_not_listed_before_private_truth_http"


@dataclass(frozen=True, slots=True)
class VenueSymbolEligibility:
    status: str
    reason: str = ""
    venue_symbol: str = ""
    catalog_loaded: bool = False
    supported_symbol_count: int = 0

    @property
    def supported(self) -> bool:
        return self.status == SUPPORTED

    @property
    def unsupported_before_http(self) -> bool:
        return self.status == UNSUPPORTED_BEFORE_HTTP


def _normalize_symbol(value: str) -> str:
    return str(value or "").upper().replace("-", "").replace("_", "")


def venue_symbol_eligibility(
    venue: Venue | str,
    symbol: str,
    *,
    supported_symbols: Iterable[str] | None,
    venue_symbol: str = "",
) -> VenueSymbolEligibility:
    """Classify whether a symbol is known tradeable before hitting symbol HTTP.

    Empty/missing catalogs are treated as unavailable truth so callers can keep
    their existing fail-closed or live-probe behavior instead of inventing a flat
    truth from missing metadata.
    """

    venue_value = venue.value if isinstance(venue, Venue) else str(venue or "").lower()
    raw_supported = [str(item or "") for item in (supported_symbols or []) if str(item or "")]
    if not raw_supported:
        return VenueSymbolEligibility(
            status=TRUTH_UNAVAILABLE,
            venue_symbol=venue_symbol or symbol,
            catalog_loaded=False,
            supported_symbol_count=0,
        )

    supported_exact = {item.upper() for item in raw_supported}
    supported_normalized = {_normalize_symbol(item) for item in raw_supported}
    candidates = {
        str(symbol or "").upper(),
        str(venue_symbol or symbol or "").upper(),
        _normalize_symbol(symbol),
        _normalize_symbol(venue_symbol or symbol),
    }
    if candidates & supported_exact or candidates & supported_normalized:
        return VenueSymbolEligibility(
            status=SUPPORTED,
            venue_symbol=venue_symbol or symbol,
            catalog_loaded=True,
            supported_symbol_count=len(raw_supported),
        )

    if venue_value == Venue.ASTER.value:
        return VenueSymbolEligibility(
            status=UNSUPPORTED_BEFORE_HTTP,
            reason=PRIVATE_TRUTH_UNSUPPORTED_REASON,
            venue_symbol=venue_symbol or symbol,
            catalog_loaded=True,
            supported_symbol_count=len(raw_supported),
        )

    return VenueSymbolEligibility(
        status=TRUTH_UNAVAILABLE,
        venue_symbol=venue_symbol or symbol,
        catalog_loaded=True,
        supported_symbol_count=len(raw_supported),
    )
