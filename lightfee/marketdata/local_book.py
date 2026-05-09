"""Local order book storage and queries."""

from __future__ import annotations

from lightfee.marketdata.l2 import L2BookStatus, LocalL2Book


def get_active_books(
    books: dict[str, LocalL2Book], max_age_ms: int, now_ms: int
) -> list[LocalL2Book]:
    """Return books that are HOT and within max age."""
    active: list[LocalL2Book] = []
    for book in books.values():
        if book.status == L2BookStatus.HOT and (now_ms - book.observed_at_ms) <= max_age_ms:
            active.append(book)
    return active
