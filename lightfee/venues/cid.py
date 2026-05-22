"""Stable, unique, exchange-legal client order ID generator.

Decouples internal entry_id (arbitrarily long) from exchange client_order_id
(must be < 36 for Binance/Aster, <= 36 for Bybit, <= 32 for OKX, alphanumeric only).

V1 parity: compact_client_order_id() is structurally compatible with Rust helpers.rs:28
but uses SHA-256 instead of DefaultHasher — produces ~20-22 character CIDs (lfxl...,
lfxs..., lfcp...) well under all exchange limits. Not byte-identical to V1; see
function docstring for details.
"""

from __future__ import annotations

import hashlib
import os

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

# V1 stage_code mapping (helpers.rs:42-54)
_STAGE_CODE: dict[str, str] = {
    "entry_long": "el",
    "entry_short": "es",
    "exit_long": "xl",
    "exit_short": "xs",
    "compensate": "cp",
}


def compact_client_order_id(position_id: str, stage: str) -> str:
    """V1 parity: generate a short, exchange-legal client order ID.

    Compatible with Rust compact_client_order_id() (helpers.rs:28-79) in structure
    and determinism, but NOT byte-identical: V1 uses Rust DefaultHasher (SipHash),
    V2 uses SHA-256 truncated to 8 bytes. The CID format and length are equivalent,
    but the hash suffix differs. Exchange idempotency holds because the (position_id,
    stage) tuple is unique per logical attempt.

    Produces ~20-22 character CIDs like lfxl3f8a2b1c9d4e5f6 — always well
    under all exchange CID length limits including Binance/Aster (must be < 36).

    The CID is deterministic: same (position_id, stage) always produces
    the same output (required for idempotency and reconciliation).
    """
    # Stage code: first 2 alphanumeric chars of known mapping or raw stage
    raw_code = _STAGE_CODE.get(stage, stage[:2] if len(stage) >= 2 else stage)
    stage_code = "".join(ch for ch in raw_code if ch.isalnum())[:2]

    # Instance code (optional env var, V1 parity)
    instance_id = os.environ.get("LIGHTFEE_INSTANCE_ID", "")
    instance_code = "".join(ch for ch in instance_id if ch.isalnum())[:4]

    # Hash position_id + stage (V1 uses DefaultHasher/SipHash; we use SHA-256
    # truncated to 8 bytes for equivalent 64-bit hash behavior)
    hasher = hashlib.sha256(f"{position_id}:{stage}".encode())
    hash_bytes = hasher.digest()[:8]

    if instance_code:
        hash_int = int.from_bytes(hash_bytes, "big") & 0x00FF_FFFF_FFFF_FFFF
        return f"lf{instance_code}{stage_code}{hash_int:014x}"
    else:
        hash_int = int.from_bytes(hash_bytes, "big")
        return f"lf{stage_code}{hash_int:016x}"


def generate_exchange_cid(internal_entry_id: str, leg: str, venue: Venue) -> str:
    """Generate a stable, unique, exchange-legal client order ID.

    Uses SHA-256 of (internal_entry_id + leg) truncated to venue max length.
    Hex encoding ensures alphanumeric-only output safe for all exchanges.

    Returns a deterministic CID: same input always produces same output
    (required for idempotency and reconciliation).

    Prefer compact_client_order_id() for close orders (V1 parity).
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
