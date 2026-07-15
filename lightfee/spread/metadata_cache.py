"""Shared fail-closed cache for slow spread contract metadata."""

from __future__ import annotations

from dataclasses import replace
from math import isfinite
from pathlib import Path
import time

from lightfee.sidecar.snapshot import QuoteSnapshot
from lightfee.spread.quote_snapshot import (
    FULL_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION,
    load_spread_quote_snapshot,
    spread_metadata_snapshot_path,
)


def quote_cache_contract_eligible(quote: QuoteSnapshot) -> bool:
    """Return whether slow metadata is complete enough for BBO overlay."""

    if quote.contract_normalization_complete is not True:
        return False
    if str(quote.contract_type or "").strip().lower() != "linear":
        return False
    if str(quote.venue_status or "").strip().lower() != "active":
        return False
    if not all(
        str(value or "").strip()
        for value in (
            quote.underlying,
            quote.quote_currency,
            quote.mark_index_source,
        )
    ):
        return False
    multiplier = quote.contract_multiplier
    if (
        isinstance(multiplier, bool)
        or not isinstance(multiplier, (int, float))
        or not isfinite(float(multiplier))
        or float(multiplier) <= 0.0
    ):
        return False
    precision_valid = all(
        isinstance(value, int) and not isinstance(value, bool) and value >= 0
        for value in (quote.price_precision, quote.quantity_precision)
    )
    exact_contract_valid = all(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and isfinite(float(value))
        and float(value) > 0.0
        for value in (
            quote.price_tick,
            quote.quantity_step_base,
            quote.min_quantity_base,
        )
    )
    min_notional_valid = bool(
        quote.min_notional_evidence_complete is True
        and isinstance(quote.min_notional_quote, (int, float))
        and not isinstance(quote.min_notional_quote, bool)
        and isfinite(float(quote.min_notional_quote))
        and float(quote.min_notional_quote) >= 0.0
    )
    schedule_valid = all(
        isinstance(value, int) and not isinstance(value, bool) and value > 0
        for value in (quote.funding_timestamp_ms, quote.funding_interval_ms)
    )
    return precision_valid and exact_contract_valid and min_notional_valid and schedule_valid


class SpreadMetadataSnapshotCache:
    """Load each metadata generation once and retain only a valid last good."""

    def __init__(self, sidecar_snapshot_path: str | Path, *, max_age_ms: int) -> None:
        self.metadata_path = spread_metadata_snapshot_path(sidecar_snapshot_path)
        self.quotes: dict[str, QuoteSnapshot] = {}
        self.published_at_ms = 0
        self.max_age_ms = max(int(max_age_ms or 0), 1)
        self._accepted_mtime_ns = 0
        self._attempted_mtime_ns = 0
        self.refresh()

    def refresh(self) -> bool:
        """Accept a changed full snapshot or retain the prior generation."""

        mtime_ns = _mtime_ns(self.metadata_path)
        if mtime_ns <= 0 or mtime_ns == self._attempted_mtime_ns:
            return False
        self._attempted_mtime_ns = mtime_ns
        snapshot = load_spread_quote_snapshot(self.metadata_path)
        if (
            snapshot is None
            or snapshot.schema_version != FULL_SPREAD_QUOTE_SNAPSHOT_SCHEMA_VERSION
            or not snapshot.quotes
        ):
            return False
        self.quotes = dict(snapshot.quotes)
        self.published_at_ms = int(snapshot.published_at_ms or 0)
        self._accepted_mtime_ns = mtime_ns
        return True

    def quote_eligible(self, quote: QuoteSnapshot, *, now_ms: int | None = None) -> bool:
        checked_at_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        quote_observed_at_ms = int(getattr(quote, "observed_at_ms", 0) or 0)
        return bool(
            self.published_at_ms > 0
            and self.published_at_ms <= checked_at_ms
            and checked_at_ms - self.published_at_ms <= self.max_age_ms
            and quote_observed_at_ms > 0
            and quote_observed_at_ms <= checked_at_ms
            and checked_at_ms - quote_observed_at_ms <= self.max_age_ms
            and quote_cache_contract_eligible(quote)
        )

    def overlay_hot_quotes(
        self,
        hot_quotes: dict[str, QuoteSnapshot],
        *,
        now_ms: int,
    ) -> tuple[dict[str, QuoteSnapshot], dict[str, set[str]]]:
        """Join volatile BBO evidence to eligible slow metadata by exact key."""

        merged: dict[str, QuoteSnapshot] = {}
        unavailable: dict[str, set[str]] = {}
        for key, hot in hot_quotes.items():
            venue = str(hot.venue or "").strip().lower()
            symbol = str(hot.symbol or "").strip().upper()
            base = self.quotes.get(key)
            if base is None or not self.quote_eligible(base, now_ms=now_ms):
                if venue and symbol:
                    unavailable.setdefault(venue, set()).add(symbol)
                continue
            merged[key] = replace(
                base,
                bid=hot.bid,
                ask=hot.ask,
                observed_at_ms=hot.observed_at_ms,
                source=hot.source,
                bid_size=hot.bid_size,
                ask_size=hot.ask_size,
                bid_depth=(),
                ask_depth=(),
            )
        return merged, unavailable


def _mtime_ns(path: Path) -> int:
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return 0
