"""Bounded, atomic bridge from the live Local-L2 process to the sidecar.

The funding sidecar and live execution runtime are separate processes.  The
sidecar must not fetch a second public order book merely to make paper fills
look realistic, so this module exports only already-validated local books and
lets the sidecar consume them as optional evidence.  A reader never repairs or
invents timestamps: malformed, stale, crossed, or partial books disappear.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterable

from lightfee.marketdata.l2 import L2BookStatus, LocalL2Book
from lightfee.sidecar.snapshot import QuoteSnapshot


LOCAL_L2_DEPTH_BRIDGE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class LocalL2Depth:
    venue: str
    symbol: str
    observed_at_ms: int
    sequence: int
    bid_depth: tuple[tuple[float, float], ...]
    ask_depth: tuple[tuple[float, float], ...]


def publish_local_l2_depth_bridge(
    path: str | Path,
    books: Iterable[LocalL2Book],
    *,
    now_ms: int,
    max_age_ms: int,
    max_levels: int,
) -> int:
    """Atomically publish usable HOT books; returns the exported book count."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    limit = max(int(max_levels or 0), 1)
    age_limit = max(int(max_age_ms or 0), 0)
    rows: list[dict[str, object]] = []
    for book in sorted(books, key=lambda item: (item.venue, item.symbol)):
        row = _book_payload(book, now_ms=now_ms, max_age_ms=age_limit, max_levels=limit)
        if row is not None:
            rows.append(row)
    payload = {
        "schema_version": LOCAL_L2_DEPTH_BRIDGE_SCHEMA_VERSION,
        "published_at_ms": int(now_ms),
        "books": rows,
    }
    _atomic_json_write(target, payload)
    return len(rows)


def load_local_l2_depth_bridge(
    path: str | Path,
    *,
    now_ms: int,
    max_age_ms: int,
) -> dict[tuple[str, str], LocalL2Depth]:
    """Load only fresh complete depth evidence, otherwise return no evidence."""
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    if int(raw.get("schema_version", 0) or 0) != LOCAL_L2_DEPTH_BRIDGE_SCHEMA_VERSION:
        return {}
    published_at_ms = _finite_nonnegative_int(raw.get("published_at_ms"))
    age_limit = max(int(max_age_ms or 0), 0)
    if published_at_ms <= 0 or now_ms < published_at_ms or now_ms - published_at_ms > age_limit:
        return {}
    records = raw.get("books")
    if not isinstance(records, list):
        return {}
    result: dict[tuple[str, str], LocalL2Depth] = {}
    for record in records:
        parsed = _parse_book(record, now_ms=now_ms, max_age_ms=age_limit)
        if parsed is None:
            continue
        result[(parsed.venue, parsed.symbol)] = parsed
    return result


def attach_local_l2_depth(
    quotes: dict[str, QuoteSnapshot],
    bridge: dict[tuple[str, str], LocalL2Depth],
    *,
    max_quote_skew_ms: int,
) -> tuple[int, int]:
    """Attach matching depth without changing the sidecar's BBO truth.

    Returns ``(attached, rejected)``.  A price or source-time mismatch is a
    rejection, not a reason to overwrite the sidecar quote: each source
    retains its own clock and paper would otherwise receive a hybrid market
    that never existed.  In particular, an unchanged BBO does not prove that
    the attached depth is still executable at that BBO.
    """
    attached = 0
    rejected = 0
    allowed_skew_ms = max(int(max_quote_skew_ms or 0), 0)
    for quote in quotes.values():
        bridge_book = bridge.get(
            (str(quote.venue).lower(), str(quote.symbol).upper())
        )
        if bridge_book is None:
            continue
        quote_observed_at_ms = _finite_nonnegative_int(quote.observed_at_ms)
        if (
            quote_observed_at_ms <= 0
            or abs(quote_observed_at_ms - bridge_book.observed_at_ms)
            > allowed_skew_ms
        ):
            rejected += 1
            continue
        if not _same_price(quote.bid, bridge_book.bid_depth[0][0]) or not _same_price(
            quote.ask, bridge_book.ask_depth[0][0]
        ):
            rejected += 1
            continue
        quote.bid_depth = bridge_book.bid_depth
        quote.ask_depth = bridge_book.ask_depth
        attached += 1
    return attached, rejected


def _book_payload(
    book: LocalL2Book,
    *,
    now_ms: int,
    max_age_ms: int,
    max_levels: int,
) -> dict[str, object] | None:
    if book.status != L2BookStatus.HOT or book.is_stale(max_age_ms, now_ms):
        return None
    bid_depth = _levels_from_book(getattr(book, "bids", ()), descending=True, max_levels=max_levels)
    ask_depth = _levels_from_book(getattr(book, "asks", ()), descending=False, max_levels=max_levels)
    if not _complete_book(bid_depth, ask_depth):
        return None
    venue = str(getattr(book, "venue", "") or "").strip().lower()
    symbol = str(getattr(book, "symbol", "") or "").strip().upper()
    observed_at_ms = _finite_nonnegative_int(getattr(book, "observed_at_ms", 0))
    if not venue or not symbol or observed_at_ms <= 0:
        return None
    return {
        "venue": venue,
        "symbol": symbol,
        "observed_at_ms": observed_at_ms,
        "sequence": _finite_nonnegative_int(getattr(book, "sequence", 0)),
        "status": "hot",
        "bid_depth": [list(level) for level in bid_depth],
        "ask_depth": [list(level) for level in ask_depth],
    }


def _parse_book(
    raw: object,
    *,
    now_ms: int,
    max_age_ms: int,
) -> LocalL2Depth | None:
    if not isinstance(raw, dict) or str(raw.get("status", "")).lower() != "hot":
        return None
    venue = str(raw.get("venue", "") or "").strip().lower()
    symbol = str(raw.get("symbol", "") or "").strip().upper()
    observed_at_ms = _finite_nonnegative_int(raw.get("observed_at_ms"))
    if (
        not venue
        or not symbol
        or observed_at_ms <= 0
        or now_ms < observed_at_ms
        or now_ms - observed_at_ms > max_age_ms
    ):
        return None
    bid_depth = _parse_levels(raw.get("bid_depth"), descending=True)
    ask_depth = _parse_levels(raw.get("ask_depth"), descending=False)
    if not _complete_book(bid_depth, ask_depth):
        return None
    return LocalL2Depth(
        venue=venue,
        symbol=symbol,
        observed_at_ms=observed_at_ms,
        sequence=_finite_nonnegative_int(raw.get("sequence")),
        bid_depth=bid_depth,
        ask_depth=ask_depth,
    )


def _levels_from_book(
    levels: object,
    *,
    descending: bool,
    max_levels: int,
) -> tuple[tuple[float, float], ...]:
    parsed: list[tuple[float, float]] = []
    if not isinstance(levels, (list, tuple)):
        return ()
    for level in levels:
        price = _finite_positive(getattr(level, "price", None))
        quantity = _finite_positive(getattr(level, "quantity", None))
        if price is None or quantity is None:
            return ()
        parsed.append((price, quantity))
        if len(parsed) >= max_levels:
            break
    if not _strictly_sorted(parsed, descending=descending):
        return ()
    return tuple(parsed)


def _parse_levels(raw: object, *, descending: bool) -> tuple[tuple[float, float], ...]:
    if not isinstance(raw, list) or not raw:
        return ()
    parsed: list[tuple[float, float]] = []
    for level in raw:
        if not isinstance(level, (list, tuple)) or len(level) != 2:
            return ()
        price = _finite_positive(level[0])
        quantity = _finite_positive(level[1])
        if price is None or quantity is None:
            return ()
        parsed.append((price, quantity))
    if not _strictly_sorted(parsed, descending=descending):
        return ()
    return tuple(parsed)


def _complete_book(
    bids: tuple[tuple[float, float], ...],
    asks: tuple[tuple[float, float], ...],
) -> bool:
    return bool(bids and asks and bids[0][0] < asks[0][0])


def _strictly_sorted(levels: list[tuple[float, float]], *, descending: bool) -> bool:
    if not levels:
        return False
    prices = [price for price, _quantity in levels]
    return all(
        current > following if descending else current < following
        for current, following in zip(prices, prices[1:])
    )


def _finite_positive(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0.0 else None


def _finite_nonnegative_int(value: object) -> int:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    if not math.isfinite(parsed) or parsed < 0.0:
        return 0
    return int(parsed)


def _atomic_json_write(path: Path, payload: dict[str, object]) -> None:
    with NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
        temporary_path = Path(handle.name)
    os.replace(temporary_path, path)


def _same_price(left: object, right: object) -> bool:
    first = _finite_positive(left)
    second = _finite_positive(right)
    if first is None or second is None:
        return False
    return abs(first - second) <= max(1e-8, max(abs(first), abs(second)) * 1e-8)
