"""Symbol universe management: directed pair filtering, daily universe loading.

V1 parity: CONFIG-001 through CONFIG-006.
Keep symbol selection here instead of scattering pair filtering in runtime code.
Startup code calls this module through the shared bootstrap boundary.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

from lightfee.config.schema import AppConfig, DirectedPairConfig
from lightfee.core.domain import Venue
from lightfee.strategy.universe import PersistedDailyUniverse

logger = logging.getLogger(__name__)

# In-memory cache of last-good symbols keyed by daily_universe.path
_last_good_cache: dict[str, list[str]] = {}


def filter_by_directed_pairs(
    pairs: list[tuple[str, str, str]],
    directed_pairs: list[DirectedPairConfig],
) -> list[tuple[str, str, str]]:
    """Filter candidate (long_venue, short_venue, symbol) triples by directed pair config.

    V1 semantics (CONFIG-001):
    - Empty directed_pairs means allow all combinations (no restriction).
    - A DirectedPairConfig with empty symbols allows all global symbols for that direction.
    - A DirectedPairConfig with symbols restricts to only those symbols.
    """
    if not directed_pairs:
        return list(pairs)

    allowed: set[tuple[str, str, str]] = set()
    for dp in directed_pairs:
        for (long, short, symbol) in pairs:
            if long != dp.long or short != dp.short:
                continue
            if not dp.symbols or symbol in dp.symbols:
                allowed.add((long, short, symbol))

    return [(l, s, sym) for (l, s, sym) in pairs if (l, s, sym) in allowed]


def validate_directed_pairs(
    directed_pairs: list[DirectedPairConfig],
    global_symbols: list[str],
) -> list[str]:
    """Validate directed_pairs consistency. Returns list of issues (empty = valid)."""
    issues: list[str] = []
    valid_venues = {v.value for v in Venue}

    for i, dp in enumerate(directed_pairs):
        if dp.long not in valid_venues:
            issues.append(f"directed_pairs[{i}].long: unknown venue '{dp.long}'")
        if dp.short not in valid_venues:
            issues.append(f"directed_pairs[{i}].short: unknown venue '{dp.short}'")
        if dp.long == dp.short:
            issues.append(f"directed_pairs[{i}]: long and short must differ")

    return issues


def _load_persisted_daily_universe(path: str) -> Optional[tuple[date, list[str]]]:
    """Read the V1 persisted daily-universe contract without judging its day."""
    try:
        persisted = PersistedDailyUniverse.load(path)
        if persisted is None:
            return None
        return date.fromisoformat(persisted.trading_date), persisted.canonical_selected_symbols()
    except (AttributeError, OSError, TypeError, ValueError):
        logger.warning("daily_universe: invalid V1 payload at %s", path)
        return None


def load_daily_universe(path: str) -> Optional[list[str]]:
    """Load today's V1-format daily universe, otherwise return ``None``.

    A JSON array without a trading date used to be treated as permanently
    current.  That silently disabled daily selection after a bad deployment.
    """
    persisted = _load_persisted_daily_universe(path)
    if persisted is None:
        return None
    trading_date, symbols = persisted
    today = date.today()
    if trading_date != today:
        logger.warning(
            "daily_universe: stale universe at %s (trading_date=%s today=%s)",
            path,
            trading_date,
            today,
        )
        return None
    return symbols


def resolve_universe_symbols(config: AppConfig) -> Optional[dict[str, Any]]:
    """Resolve runtime trading symbols from daily universe or static config.

    V1 semantics (CONFIG-002, CONFIG-005):
    - If daily_universe.enabled, load from daily_universe.path.
    - If missing and fallback_to_last_good, use last known good universe.
    - Enforce max_symbols cap.
    - If daily_universe disabled or unavailable with no fallback, use config.symbols.

    Returns a dict with keys:
      daily_universe_enabled, global_symbol_count, resolved_symbol_count,
      resolved_symbols, used_fallback
    """
    daily = config.runtime.daily_universe

    if not daily.enabled:
        symbols = list(config.symbols)
        return {
            "daily_universe_enabled": False,
            "global_symbol_count": len(symbols),
            "resolved_symbol_count": len(symbols),
            "resolved_symbols": symbols,
            "used_fallback": False,
        }

    symbols: Optional[list[str]] = None
    used_fallback = False
    today = date.today()

    persisted = _load_persisted_daily_universe(daily.path)
    if persisted is not None and persisted[0] == today:
        symbols = list(persisted[1])
    elif persisted is not None and daily.fallback_to_last_good:
        # V1 first tries to generate today's universe.  V2 resolves before
        # adapters exist, so a stale validated persisted selection is the
        # bounded fallback rather than pretending it is today's selection.
        symbols = list(persisted[1])
        used_fallback = True
        logger.warning(
            "daily_universe: using stale validated fallback (%d symbols)",
            len(symbols),
        )

    if symbols is None and daily.fallback_to_last_good:
        cached = _last_good_cache.get(daily.path)
        if cached:
            symbols = list(cached)
            used_fallback = True
            logger.info("daily_universe: using last-good fallback (%d symbols)", len(symbols))

    if symbols is None and daily.fallback_to_last_good:
        # No cache either — fall back to static symbols
        symbols = list(config.symbols)
        used_fallback = True
        logger.warning(
            "daily_universe: no universe file and no cached last-good; "
            "falling back to static symbols (%d symbols)",
            len(symbols),
        )

    if symbols is None:
        symbols = list(config.symbols)
        used_fallback = True

    # Enforce max_symbols cap (CONFIG-005)
    if len(symbols) > daily.max_symbols:
        logger.warning(
            "daily_universe: capping symbols from %d to max_symbols=%d",
            len(symbols),
            daily.max_symbols,
        )
        symbols = symbols[: daily.max_symbols]

    # Cache as last good for future fallback
    if not used_fallback and symbols:
        _last_good_cache[daily.path] = list(symbols)

    return {
        "daily_universe_enabled": True,
        "global_symbol_count": len(config.symbols),
        "resolved_symbol_count": len(symbols),
        "resolved_symbols": symbols,
        "used_fallback": used_fallback,
    }
