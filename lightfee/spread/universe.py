"""Bound the spread hot path to executable, cross-venue symbols."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from itertools import combinations
from math import isclose, isfinite

from lightfee.config.schema import AppConfig
from lightfee.config.universe import load_daily_universe
from lightfee.sidecar.snapshot import QuoteSnapshot


# Robust rolling statistics are O(pair_count * history_length).  A global
# funding universe is therefore not a valid spread hot-path universe.  This
# bound keeps the paper sampler within a sub-second scheduling budget while
# retaining the symbols with the strongest conservative cross-venue liquidity.
SPREAD_SAMPLING_MAX_PAIR_COUNT = 384


def spread_sampling_pair_bound(
    symbols: Iterable[str],
    venues: Iterable[str],
) -> int:
    """Return the worst-case pair count for one frozen symbol universe.

    The bound deliberately charges every declared symbol for every configured
    venue pair.  A temporarily missing exchange must not make the universe look
    cheap at startup and then silently expand the signal workload when that
    exchange recovers.
    """

    symbol_count = len(
        {
            str(symbol).strip().upper()
            for symbol in symbols
            if str(symbol).strip()
        }
    )
    venue_count = len(
        {
            str(venue).strip().lower()
            for venue in venues
            if str(venue).strip()
        }
    )
    return symbol_count * (venue_count * (venue_count - 1) // 2)


def spread_sampling_selection_required(config: AppConfig) -> bool:
    """Return whether spread needs an independently selected bounded universe."""

    return bool(
        config.runtime.daily_universe.enabled
        or spread_sampling_pair_bound(
            config.symbols,
            (venue.venue for venue in config.venues),
        )
        > SPREAD_SAMPLING_MAX_PAIR_COUNT
    )


def resolve_spread_sampling_symbols(
    config: AppConfig,
    metadata_quotes: dict[str, QuoteSnapshot],
    *,
    quote_eligible: Callable[[QuoteSnapshot], bool],
) -> tuple[str, ...]:
    """Resolve one deterministic process-lifetime spread sampling universe.

    The funding universe remains untouched.  A configured symbol list that
    already fits the hard pair budget is preserved exactly.  An oversized list
    is independently ranked even when funding's daily-universe feature is off;
    when that feature is on, its file is only an optional allow-list. Ranking
    uses the second-best compatible venue's 24h quote volume so one liquid venue
    cannot hide an unusable hedge leg.
    """

    configured = tuple(
        dict.fromkeys(
            str(symbol).strip().upper()
            for symbol in config.symbols
            if str(symbol).strip()
        )
    )
    daily = config.runtime.daily_universe
    if not spread_sampling_selection_required(config):
        return configured

    allowed = set(configured)
    if daily.enabled:
        daily_symbols = load_daily_universe(daily.path)
        if daily_symbols is not None:
            allowed.intersection_update(
                str(symbol).strip().upper()
                for symbol in daily_symbols
                if str(symbol).strip()
            )
    if not allowed:
        return ()

    configured_venues = {
        str(venue.venue).strip().lower()
        for venue in config.venues
        if str(venue.venue).strip()
    }
    pair_cost_per_symbol = spread_sampling_pair_bound(("_",), configured_venues)
    if pair_cost_per_symbol <= 0:
        return ()

    quotes_by_symbol: dict[str, dict[str, tuple[QuoteSnapshot, float]]] = {}
    for quote in metadata_quotes.values():
        symbol = str(getattr(quote, "symbol", "") or "").strip().upper()
        venue = str(getattr(quote, "venue", "") or "").strip().lower()
        if (
            symbol not in allowed
            or venue not in configured_venues
            or not quote_eligible(quote)
        ):
            continue
        raw_volume = getattr(quote, "volume_24h_quote", 0.0)
        try:
            volume = float(raw_volume or 0.0)
        except (TypeError, ValueError, OverflowError):
            volume = 0.0
        if not isfinite(volume) or volume < 0.0:
            volume = 0.0
        quotes_by_symbol.setdefault(symbol, {})[venue] = (quote, volume)

    ranked: list[tuple[float, int, str]] = []
    for symbol, quotes_by_venue in quotes_by_symbol.items():
        visible_venue_count = len(quotes_by_venue)
        if visible_venue_count < 2:
            continue
        # Score only economically compatible legs.  Counting two high-volume
        # venues with different underlyings, quotes or contract multipliers
        # would spend the sampling budget on a pair the signal engine must
        # reject.  Pair cost is charged separately against *all configured*
        # venues so a later venue recovery cannot expand the frozen budget.
        conservative_volume = max(
            (
                min(left_volume, right_volume)
                for (left, left_volume), (right, right_volume) in combinations(
                    quotes_by_venue.values(), 2
                )
                if _same_executable_contract(left, right)
            ),
            default=0.0,
        )
        if conservative_volume <= 0.0:
            continue
        ranked.append((-conservative_volume, -visible_venue_count, symbol))
    ranked.sort()

    symbol_limit = max(int(daily.max_symbols or 0), 1)
    selected: list[str] = []
    pair_count = 0
    for _negative_volume, _negative_venue_count, symbol in ranked:
        if len(selected) >= symbol_limit:
            break
        if pair_count + pair_cost_per_symbol > SPREAD_SAMPLING_MAX_PAIR_COUNT:
            continue
        selected.append(symbol)
        pair_count += pair_cost_per_symbol
    return tuple(selected)


def _same_executable_contract(left: QuoteSnapshot, right: QuoteSnapshot) -> bool:
    """Match the pairwise economic identity enforced by the signal engine."""

    for field_name in ("underlying", "quote_currency", "contract_type"):
        left_value = str(getattr(left, field_name, "") or "").strip().lower()
        right_value = str(getattr(right, field_name, "") or "").strip().lower()
        if not left_value or left_value != right_value:
            return False
    try:
        left_multiplier = float(left.contract_multiplier or 0.0)
        right_multiplier = float(right.contract_multiplier or 0.0)
    except (TypeError, ValueError, OverflowError):
        return False
    if not isfinite(left_multiplier) or not isfinite(right_multiplier):
        return False
    return isclose(left_multiplier, right_multiplier, rel_tol=1e-12)
