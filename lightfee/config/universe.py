"""Symbol universe management: directed pair filtering, daily universe loading.

V1 parity: CONFIG-001 through CONFIG-006.
Keep symbol selection here instead of scattering pair filtering in runtime code.
Startup code calls this module through the shared bootstrap boundary.
"""

from __future__ import annotations

import logging
import math
import time
from datetime import date
from typing import Any, Awaitable, Callable, Optional

from lightfee.config.schema import AppConfig, DirectedPairConfig
from lightfee.core.domain import Venue
from lightfee.strategy.universe import PersistedDailyUniverse, today_trading_date

logger = logging.getLogger(__name__)

# In-memory cache of last-good symbols keyed by daily_universe.path
_last_good_cache: dict[str, list[str]] = {}


def _today() -> date:
    """Use the same Shanghai trading date for generation and resolution."""
    return date.fromisoformat(today_trading_date())


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
    today = _today()
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
    """Read a resolved runtime universe or a validated last-good fallback.

    V1 semantics (CONFIG-002, CONFIG-005):
    - If daily_universe.enabled, load from daily_universe.path.
    - If missing and fallback_to_last_good, use a known-good persisted or
      in-memory universe.
    - Enforce max_symbols cap.
    - If daily_universe is enabled but no current or last-good universe exists,
      fail instead of silently expanding to static config.symbols.

    ``resolve_or_generate_universe_symbols`` owns generation.  This low-level
    resolver is intentionally read/fallback-only so every live startup must
    cross the same generate-before-fallback boundary.

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
    today = _today()

    persisted = _load_persisted_daily_universe(daily.path)
    if persisted is not None and persisted[0] == today:
        symbols = list(persisted[1])
    elif persisted is not None and daily.fallback_to_last_good:
        # A generation attempt has already failed at the runtime boundary.
        # A stale validated selection is bounded and does not pretend to be
        # today's snapshot.
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

    if symbols is None:
        raise RuntimeError("daily universe unavailable and no last-good fallback")

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


def _daily_universe_is_current(config: AppConfig) -> bool:
    daily = config.runtime.daily_universe
    if not daily.enabled:
        return False
    persisted = _load_persisted_daily_universe(daily.path)
    return persisted is not None and persisted[0] == _today()


def _directed_pairs_or_all(config: AppConfig) -> list[tuple[str, str, list[str]]]:
    directed_pairs = config.runtime.directed_pairs
    if directed_pairs:
        return [
            (str(pair.long).lower(), str(pair.short).lower(), list(pair.symbols))
            for pair in directed_pairs
        ]

    venues = list(dict.fromkeys(str(venue.venue).lower() for venue in config.venues))
    return [
        (long_venue, short_venue, [])
        for long_venue in venues
        for short_venue in venues
        if long_venue != short_venue
    ]


def _select_daily_universe_symbols(
    config: AppConfig,
    liquidity_by_venue: dict[str, dict[str, Any]],
) -> list[str]:
    """Apply the V1 cross-venue liquidity contract to public market rows."""
    source_symbols = list(config.symbols)
    source_by_canonical = {str(symbol).upper(): str(symbol).upper() for symbol in source_symbols}
    pairs = _directed_pairs_or_all(config)
    if not pairs:
        return source_symbols[: config.runtime.daily_universe.max_symbols]

    normalized_rows: dict[str, dict[str, Any]] = {}
    for venue, rows in liquidity_by_venue.items():
        normalized_rows[venue.lower()] = {
            str(getattr(row, "symbol", symbol)).upper(): row
            for symbol, row in rows.items()
        }

    scored: dict[str, tuple[float, float]] = {}
    usable_pair_count = 0
    for long_venue, short_venue, configured_symbols in pairs:
        long_rows = normalized_rows.get(long_venue)
        short_rows = normalized_rows.get(short_venue)
        if long_rows is None or short_rows is None:
            continue
        usable_pair_count += 1
        allowed_symbols = (
            [str(symbol).upper() for symbol in configured_symbols]
            if configured_symbols
            else list(source_by_canonical)
        )
        for symbol in allowed_symbols:
            if symbol not in source_by_canonical:
                continue
            long_row = long_rows.get(symbol)
            short_row = short_rows.get(symbol)
            if long_row is None or short_row is None:
                continue
            long_volume = float(getattr(long_row, "volume_24h_quote", 0.0) or 0.0)
            short_volume = float(getattr(short_row, "volume_24h_quote", 0.0) or 0.0)
            long_open_interest = float(getattr(long_row, "open_interest_quote", 0.0) or 0.0)
            short_open_interest = float(getattr(short_row, "open_interest_quote", 0.0) or 0.0)
            if not all(
                math.isfinite(value)
                for value in (
                    long_volume,
                    short_volume,
                    long_open_interest,
                    short_open_interest,
                )
            ):
                continue
            if (
                long_volume < config.strategy.entry_volume_floor_quote(long_venue)
                or short_volume < config.strategy.entry_volume_floor_quote(short_venue)
                or long_open_interest < config.strategy.entry_open_interest_floor_quote(long_venue)
                or short_open_interest < config.strategy.entry_open_interest_floor_quote(short_venue)
            ):
                continue
            score = (min(long_volume, short_volume), min(long_open_interest, short_open_interest))
            if score > scored.get(symbol, (-1.0, -1.0)):
                scored[symbol] = score

    if usable_pair_count == 0:
        raise RuntimeError("no directed venue pair returned daily-universe liquidity")

    ranked = sorted(
        scored.items(),
        key=lambda item: (-item[1][0], -item[1][1], item[0]),
    )
    max_symbols = config.runtime.daily_universe.max_symbols
    return [symbol for symbol, _score in ranked[:max_symbols]]


async def resolve_or_generate_universe_symbols(
    config: AppConfig,
    fetch_liquidity: Callable[[list[str]], Awaitable[dict[str, dict[str, Any]]]],
) -> dict[str, Any]:
    """Resolve today's persisted universe or create it from public liquidity.

    This is the sole runtime owner of the V1 generate-before-fallback contract.
    The caller supplies public, bounded market acquisition; this module owns the
    selection, persistence, and fallback semantics shared by live and sidecar.
    """
    daily = config.runtime.daily_universe
    if not daily.enabled or _daily_universe_is_current(config):
        return resolve_universe_symbols(config)

    try:
        liquidity_by_venue = await fetch_liquidity(list(config.symbols))
        selected_symbols = _select_daily_universe_symbols(config, liquidity_by_venue)
        # Another process may have completed the day's atomic write while this
        # process fetched public data. Prefer that established selection.
        if not _daily_universe_is_current(config):
            PersistedDailyUniverse(
                trading_date=today_trading_date(),
                generated_at_ms=int(time.time() * 1000),
                source_symbol_count=len(config.symbols),
                selected_symbol_count=len(selected_symbols),
                selected_symbols=selected_symbols,
            ).save(daily.path)
    except Exception as exc:
        if not daily.fallback_to_last_good:
            raise RuntimeError("daily universe generation failed") from exc
        logger.warning("daily_universe: generation failed; using fallback: %s", exc)
        try:
            return resolve_universe_symbols(config)
        except RuntimeError:
            raise RuntimeError(
                "daily universe generation failed without last-good fallback"
            ) from exc

    return resolve_universe_symbols(config)
