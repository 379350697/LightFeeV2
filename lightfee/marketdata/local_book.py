"""Local order book storage, health queries, and pool management."""

from __future__ import annotations

from lightfee.marketdata.l2 import L2BookStatus, L2PoolAssignment, LocalL2Book


def get_active_books(
    books: dict[str, LocalL2Book], max_age_ms: int, now_ms: int
) -> list[LocalL2Book]:
    """Return books that are HOT and within max age."""
    active: list[LocalL2Book] = []
    for book in books.values():
        if book.status == L2BookStatus.HOT and (now_ms - book.observed_at_ms) <= max_age_ms:
            active.append(book)
    return active


def get_books_by_pool(
    books: dict[str, LocalL2Book], pool: L2PoolAssignment
) -> list[LocalL2Book]:
    """Return all books assigned to a given pool."""
    return [b for b in books.values() if b.pool == pool]


def get_unhealthy_books(
    books: dict[str, LocalL2Book],
) -> list[LocalL2Book]:
    """Return books that are not healthy (degraded, suspended, etc.)."""
    return [b for b in books.values() if not b.is_healthy()]


def get_stale_books(
    books: dict[str, LocalL2Book], max_age_ms: int, now_ms: int
) -> list[LocalL2Book]:
    """Return books past their freshness threshold."""
    return [b for b in books.values() if b.is_stale(max_age_ms, now_ms)]


def count_by_status(books: dict[str, LocalL2Book]) -> dict[L2BookStatus, int]:
    """Count books per status category."""
    counts: dict[L2BookStatus, int] = {}
    for book in books.values():
        counts[book.status] = counts.get(book.status, 0) + 1
    return counts
