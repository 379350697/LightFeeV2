"""Stable, unique, exchange-legal client order ID generator.

Decouples internal entry_id (arbitrarily long) from exchange client_order_id
(must be <= 36 for Binance/Aster/Bybit, <= 32 for OKX, alphanumeric only).
"""

from __future__ import annotations

import hashlib

from lightfee.core.domain import Venue

# Allowed char sets per exchange (we use hex which is safe for all)
_CID_MAX_LEN: dict[Venue, int] = {
    Venue.BINANCE: 36,
    Venue.ASTER: 36,
    Venue.BYBIT: 36,
    Venue.OKX: 32,
    Venue.BITGET: 36,
    Venue.GATE: 36,
    Venue.HYPERLIQUID: 36,
}


def generate_exchange_cid(internal_entry_id: str, leg: str, venue: Venue) -> str:
    """Generate a stable, unique, exchange-legal client order ID.

    Uses SHA-256 of (internal_entry_id + leg) truncated to venue max length.
    Hex encoding ensures alphanumeric-only output safe for all exchanges.

    Returns a deterministic CID: same input always produces same output
    (required for idempotency and reconciliation).
    """
    max_len = _CID_MAX_LEN.get(venue, 36)
    digest = hashlib.sha256(f"{internal_entry_id}:{leg}".encode()).digest()
    byte_len = max_len // 2  # hex = 2 chars per byte
    return digest[:byte_len].hex()


def cid_is_valid_for_venue(cid: str, venue: Venue) -> bool:
    """Check whether a CID is legal for the given venue."""
    max_len = _CID_MAX_LEN.get(venue, 36)
    if len(cid) > max_len:
        return False
    if not cid:
        return False
    return all(c.isalnum() or c in "-_" for c in cid)
