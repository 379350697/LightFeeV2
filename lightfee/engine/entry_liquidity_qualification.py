from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from math import isfinite
from typing import Any


ENTRY_LIQUIDITY_STRUCTURAL_FAILURE_THRESHOLD = 3
ENTRY_LIQUIDITY_STRUCTURAL_SUPPRESS_MS = 30 * 60 * 1_000
ENTRY_LIQUIDITY_STRUCTURAL_PROBE_INTERVAL_MS = 60 * 1_000


class EntryLiquidityEligibilityClass(str, Enum):
    ELIGIBLE = "eligible"
    TEMPORARY_BELOW_FLOOR = "temporary_below_floor"
    STRUCTURAL_INELIGIBILITY = "structural_ineligibility"
    DATA_UNAVAILABLE = "data_unavailable"


@dataclass
class VenueSymbolEligibilityWindow:
    consecutive_failures: int = 0
    last_failure_at_ms: int | None = None
    suppress_until_ms: int | None = None
    last_class: EntryLiquidityEligibilityClass | None = None
    last_observed_open_interest_quote: int | None = None
    last_observed_open_interest_at_ms: int | None = None
    last_structural_probe_at_ms: int | None = None


class EntryLiquidityQualificationState:
    def __init__(self) -> None:
        self._windows: dict[tuple[str, str], VenueSymbolEligibilityWindow] = {}

    @classmethod
    def from_records(cls, records: list[dict[str, Any]] | None) -> "EntryLiquidityQualificationState":
        state = cls()
        for record in records or []:
            venue = _venue_key(record.get("venue", ""))
            symbol = _symbol_key(record.get("symbol", ""))
            if not venue or not symbol:
                continue
            state._windows[(venue, symbol)] = VenueSymbolEligibilityWindow(
                consecutive_failures=max(int(record.get("consecutive_failures", 0) or 0), 0),
                last_failure_at_ms=_optional_int(record.get("last_failure_at_ms")),
                suppress_until_ms=_optional_int(record.get("suppress_until_ms")),
                last_class=_eligibility_class(record.get("last_class")),
                last_observed_open_interest_quote=_optional_int(
                    record.get("last_observed_open_interest_quote")
                ),
                last_observed_open_interest_at_ms=_optional_int(
                    record.get("last_observed_open_interest_at_ms")
                ),
                last_structural_probe_at_ms=_optional_int(
                    record.get("last_structural_probe_at_ms")
                ),
            )
        return state

    def record_result(
        self,
        venue,
        symbol,
        klass: EntryLiquidityEligibilityClass | str,
        *,
        now_ms: int,
        threshold_failures: int = ENTRY_LIQUIDITY_STRUCTURAL_FAILURE_THRESHOLD,
        suppress_for_ms: int = ENTRY_LIQUIDITY_STRUCTURAL_SUPPRESS_MS,
    ) -> EntryLiquidityEligibilityClass:
        result_class = _eligibility_class(klass) or EntryLiquidityEligibilityClass.ELIGIBLE
        window = self._window(venue, symbol)
        window.last_class = result_class

        if result_class is EntryLiquidityEligibilityClass.ELIGIBLE:
            window.consecutive_failures = 0
            window.last_failure_at_ms = None
            window.suppress_until_ms = None
            window.last_structural_probe_at_ms = None
            return EntryLiquidityEligibilityClass.ELIGIBLE

        if result_class is EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR:
            window.consecutive_failures = max(window.consecutive_failures, 0) + 1
            window.last_failure_at_ms = int(now_ms)
            if window.consecutive_failures >= max(int(threshold_failures), 1):
                window.suppress_until_ms = int(now_ms) + max(int(suppress_for_ms), 0)
                window.last_class = EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY
                return EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY
            return EntryLiquidityEligibilityClass.TEMPORARY_BELOW_FLOOR

        if result_class is EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY:
            window.consecutive_failures = max(window.consecutive_failures, 0) + 1
            window.last_failure_at_ms = int(now_ms)
            window.suppress_until_ms = int(now_ms) + max(int(suppress_for_ms), 0)
            window.last_structural_probe_at_ms = None
            return EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY

        window.last_failure_at_ms = int(now_ms)
        return EntryLiquidityEligibilityClass.DATA_UNAVAILABLE

    def current_class(
        self,
        venue,
        symbol,
        *,
        now_ms: int,
    ) -> EntryLiquidityEligibilityClass:
        window = self._windows.get((_venue_key(venue), _symbol_key(symbol)))
        if window is None:
            return EntryLiquidityEligibilityClass.ELIGIBLE
        if (
            window.suppress_until_ms is not None
            and int(window.suppress_until_ms) >= int(now_ms)
        ):
            return EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY
        if window.last_class is EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY:
            return EntryLiquidityEligibilityClass.ELIGIBLE
        return window.last_class or EntryLiquidityEligibilityClass.ELIGIBLE

    def should_probe_structural(
        self,
        venue,
        symbol,
        *,
        now_ms: int,
        probe_interval_ms: int = ENTRY_LIQUIDITY_STRUCTURAL_PROBE_INTERVAL_MS,
    ) -> bool:
        window = self._window(venue, symbol)
        suppress_active = (
            window.suppress_until_ms is not None
            and int(window.suppress_until_ms) >= int(now_ms)
            and window.last_class is EntryLiquidityEligibilityClass.STRUCTURAL_INELIGIBILITY
        )
        if not suppress_active:
            return True
        if (
            window.last_structural_probe_at_ms is not None
            and int(now_ms) - int(window.last_structural_probe_at_ms) < max(int(probe_interval_ms), 0)
        ):
            return False
        window.last_structural_probe_at_ms = int(now_ms)
        return True

    def note_open_interest_observation(
        self,
        venue,
        symbol,
        open_interest_quote: float,
        *,
        observed_at_ms: int,
    ) -> None:
        encoded = _encode_open_interest_quote(open_interest_quote)
        if encoded is None:
            return
        window = self._window(venue, symbol)
        window.last_observed_open_interest_quote = encoded
        window.last_observed_open_interest_at_ms = max(int(observed_at_ms), 0)

    def last_observed_open_interest_quote(self, venue, symbol) -> float | None:
        window = self._windows.get((_venue_key(venue), _symbol_key(symbol)))
        if window is None or window.last_observed_open_interest_quote is None:
            return None
        return float(window.last_observed_open_interest_quote)

    def to_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for (venue, symbol), window in sorted(self._windows.items()):
            records.append({
                "venue": venue,
                "symbol": symbol,
                "consecutive_failures": int(window.consecutive_failures),
                "last_failure_at_ms": window.last_failure_at_ms,
                "suppress_until_ms": window.suppress_until_ms,
                "last_class": window.last_class.value if window.last_class else None,
                "last_observed_open_interest_quote": window.last_observed_open_interest_quote,
                "last_observed_open_interest_at_ms": window.last_observed_open_interest_at_ms,
                "last_structural_probe_at_ms": window.last_structural_probe_at_ms,
            })
        return records

    def _window(self, venue, symbol) -> VenueSymbolEligibilityWindow:
        key = (_venue_key(venue), _symbol_key(symbol))
        window = self._windows.get(key)
        if window is None:
            window = VenueSymbolEligibilityWindow()
            self._windows[key] = window
        return window


def _venue_key(value) -> str:
    if hasattr(value, "value"):
        value = value.value
    return str(value or "").lower()


def _symbol_key(value) -> str:
    return str(value or "").upper()


def _eligibility_class(value) -> EntryLiquidityEligibilityClass | None:
    if isinstance(value, EntryLiquidityEligibilityClass):
        return value
    value_s = str(value or "").lower()
    for klass in EntryLiquidityEligibilityClass:
        if klass.value == value_s:
            return klass
    return None


def _optional_int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _encode_open_interest_quote(value: float) -> int | None:
    value_f = float(value)
    if not isfinite(value_f) or value_f < 0.0:
        return None
    return int(round(min(value_f, float(2**63 - 1))))
