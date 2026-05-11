"""Local order book storage, health queries, and pool management.

Rust V1 parity: book filtering, counting, and readiness queries used by
local-L2 runtime, entry sessions, and execution liquidity.
"""

from __future__ import annotations

from lightfee.marketdata.l2 import (
    L2BookStatus,
    L2PoolAssignment,
    LocalL2Book,
    LocalL2BookKey,
)


def get_active_books(
    books: dict[LocalL2BookKey, LocalL2Book], max_age_ms: int, now_ms: int
) -> list[LocalL2Book]:
    """Return books that are HOT and within max age."""
    return [
        b for b in books.values()
        if b.status == L2BookStatus.HOT and (now_ms - b.observed_at_ms) <= max_age_ms
    ]


def get_ready_books(
    books: dict[LocalL2BookKey, LocalL2Book], max_age_ms: int, now_ms: int
) -> list[LocalL2Book]:
    """Return books that pass is_ready() — HOT and within freshness window."""
    return [b for b in books.values() if b.is_ready(max_age_ms, now_ms)]


def get_books_by_pool(
    books: dict[LocalL2BookKey, LocalL2Book], pool: L2PoolAssignment
) -> list[LocalL2Book]:
    """Return all books assigned to a given pool."""
    return [b for b in books.values() if b.pool == pool]


def get_books_by_venue(
    books: dict[LocalL2BookKey, LocalL2Book], venue: str
) -> list[LocalL2Book]:
    """Return all books for a given venue."""
    return [b for b in books.values() if b.venue == venue]


def get_unhealthy_books(
    books: dict[LocalL2BookKey, LocalL2Book],
) -> list[LocalL2Book]:
    """Return books that are not healthy (degraded, suspended, etc.)."""
    return [b for b in books.values() if not b.is_healthy()]


def get_stale_books(
    books: dict[LocalL2BookKey, LocalL2Book], max_age_ms: int, now_ms: int
) -> list[LocalL2Book]:
    """Return books past their freshness threshold."""
    return [b for b in books.values() if b.is_stale(max_age_ms, now_ms)]


def get_resume_waiting_books(
    books: dict[LocalL2BookKey, LocalL2Book],
) -> list[LocalL2Book]:
    """Return books in RESUME_WAITING status."""
    return [b for b in books.values() if b.status == L2BookStatus.RESUME_WAITING]


def count_by_status(books: dict[LocalL2BookKey, LocalL2Book]) -> dict[L2BookStatus, int]:
    """Count books per status category."""
    counts: dict[L2BookStatus, int] = {}
    for book in books.values():
        counts[book.status] = counts.get(book.status, 0) + 1
    return counts


def count_by_pool(books: dict[LocalL2BookKey, LocalL2Book]) -> dict[L2PoolAssignment, int]:
    """Count books per pool assignment."""
    counts: dict[L2PoolAssignment, int] = {}
    for book in books.values():
        counts[book.pool] = counts.get(book.pool, 0) + 1
    return counts


def find_book(
    books: dict[LocalL2BookKey, LocalL2Book], venue: str, symbol: str
) -> LocalL2Book | None:
    """Find a book by venue and symbol."""
    return books.get(LocalL2BookKey(venue=venue, symbol=symbol))


def book_key(venue: str, symbol: str) -> LocalL2BookKey:
    """Convenience: build a book key."""
    return LocalL2BookKey(venue=venue, symbol=symbol)


def books_for_symbol(
    books: dict[LocalL2BookKey, LocalL2Book], symbol: str
) -> list[LocalL2Book]:
    """Return all books for a given symbol across venues."""
    return [b for b in books.values() if b.symbol == symbol]


def hot_exec_ready_count(
    books: dict[LocalL2BookKey, LocalL2Book], max_age_ms: int, now_ms: int
) -> int:
    """Count HOT_EXEC books that are ready (HOT and fresh)."""
    return sum(
        1 for b in books.values()
        if b.pool == L2PoolAssignment.HOT_EXEC and b.is_ready(max_age_ms, now_ms)
    )
