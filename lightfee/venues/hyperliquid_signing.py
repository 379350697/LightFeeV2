"""Hyperliquid EIP-712 L1 action signing — matches Rust hyperliquid_rust_sdk output."""

from __future__ import annotations

import time
import hashlib
from typing import Any, Optional

from Crypto.Hash import keccak as _keccak

try:
    import msgpack
except ImportError:
    msgpack = None  # type: ignore

try:
    from eth_account import Account
    from eth_account.messages import encode_typed_data
except ImportError:
    Account = None  # type: ignore
    encode_typed_data = None  # type: ignore


# ---------------------------------------------------------------------------
# EIP-712 domain / types — identical to Rust Agent struct
# ---------------------------------------------------------------------------

_DOMAIN = {
    "name": "Exchange",
    "version": "1",
    "chainId": 1337,
    "verifyingContract": "0x0000000000000000000000000000000000000000",
}

_TYPES: dict[str, list[dict[str, str]]] = {
    "EIP712Domain": [
        {"name": "name", "type": "string"},
        {"name": "version", "type": "string"},
        {"name": "chainId", "type": "uint256"},
        {"name": "verifyingContract", "type": "address"},
    ],
    "Agent": [
        {"name": "source", "type": "string"},
        {"name": "connectionId", "type": "bytes32"},
    ],
}


def _keccak256(data: bytes) -> bytes:
    k = _keccak.new(digest_bits=256)
    k.update(data)
    return k.digest()


# ---------------------------------------------------------------------------
# Nonce — monotonic, clock-based (matches Rust next_hyperliquid_nonce)
# ---------------------------------------------------------------------------

_NONCE_STATE: int = 0


def next_hyperliquid_nonce() -> int:
    """Monotonic nonce: max(current, now_ms) + 1. Thread-safe only per-process."""
    global _NONCE_STATE
    now = int(time.time() * 1000)
    if _NONCE_STATE < now:
        _NONCE_STATE = now
    _NONCE_STATE += 1
    return _NONCE_STATE


# ---------------------------------------------------------------------------
# connection_id — keccak256(msgpack(action) || nonce_be || vault_flag)
# ---------------------------------------------------------------------------


def hyperliquid_connection_id(
    action: dict[str, Any],
    nonce: int,
    vault_address: Optional[str] = None,
) -> str:
    """Compute connection_id: keccak(msgpack(action) + nonce_be_bytes + vault_flag).

    Matches Rust: hyperliquid_action_connection_id()
    """
    if msgpack is None:
        raise ImportError("msgpack is required for Hyperliquid signing")

    packed = msgpack.packb(action)
    data = bytearray(packed)
    data.extend(nonce.to_bytes(8, "big"))
    if vault_address is not None:
        data.append(1)
        data.extend(bytes.fromhex(vault_address.replace("0x", "")))
    else:
        data.append(0)
    return "0x" + _keccak256(bytes(data)).hex()


# ---------------------------------------------------------------------------
# sign_l1_action — EIP-712 sign the Agent struct
# ---------------------------------------------------------------------------


def sign_hyperliquid_l1_action(
    private_key_hex: str,
    connection_id_hex: str,
    is_mainnet: bool = True,
) -> dict[str, Any]:
    """Sign an L1 action via EIP-712 Agent typed data.

    Returns dict with r, s, v, and signature hex string.
    Matches Rust: sign_hyperliquid_l1_action()
    """
    if Account is None or encode_typed_data is None:
        raise ImportError("eth-account is required for Hyperliquid signing")

    source = "a" if is_mainnet else "b"
    message = {"source": source, "connectionId": connection_id_hex}

    encoded = encode_typed_data(
        full_message={
            "types": _TYPES,
            "domain": _DOMAIN,
            "primaryType": "Agent",
            "message": message,
        }
    )

    acct = Account.from_key(private_key_hex)
    signed = acct.sign_message(encoded)
    r_hex = signed.r.to_bytes(32, "big").hex()
    s_hex = signed.s.to_bytes(32, "big").hex()
    v_int = signed.v
    sig_hex = r_hex + s_hex + hex(v_int)[2:]

    return {"r": hex(signed.r), "s": hex(signed.s), "v": v_int, "signature": sig_hex}


# ---------------------------------------------------------------------------
# Build exchange payload
# ---------------------------------------------------------------------------


def build_hyperliquid_exchange_payload(
    action: dict[str, Any],
    private_key_hex: str,
    vault_address: Optional[str] = None,
    is_mainnet: bool = True,
    cloid: Optional[str] = None,
) -> dict[str, Any]:
    """Build a complete signed Hyperliquid exchange payload.

    Returns the JSON-serializable dict ready for POST /exchange or WS.
    The client order id belongs in each order's ``c`` field, not the top level.
    """
    nonce = next_hyperliquid_nonce()
    conn_id = hyperliquid_connection_id(action, nonce, vault_address)
    sig = sign_hyperliquid_l1_action(private_key_hex, conn_id, is_mainnet)

    payload: dict[str, Any] = {
        "action": action,
        "signature": {
            "r": sig["r"],
            "s": sig["s"],
            "v": sig["v"],
        },
        "nonce": nonce,
    }
    if vault_address is not None:
        payload["vaultAddress"] = vault_address

    return payload


def is_hyperliquid_wire_cloid(value: str) -> bool:
    """Return true when ``value`` already matches Hyperliquid's 128-bit cloid."""
    text = str(value or "").strip()
    if len(text) != 34 or not text.startswith("0x"):
        return False
    try:
        int(text[2:], 16)
    except ValueError:
        return False
    return True


def hyperliquid_cloid_for_client_order(client_order_id: str) -> str:
    """V1 parity: map internal client ids to Hyperliquid 128-bit hex cloids."""
    text = str(client_order_id or "").strip()
    if is_hyperliquid_wire_cloid(text):
        return text
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return "0x" + digest[:16].hex()


# ---------------------------------------------------------------------------
# Order action construction
# ---------------------------------------------------------------------------


def build_hyperliquid_order_action(
    symbol: str,
    is_buy: bool,
    quantity: float,
    price: float,
    reduce_only: bool = False,
    tif: str = "Ioc",
    cloid: Optional[str] = None,
) -> dict[str, Any]:
    """Build an unsigned Hyperliquid order action dict.

    The ``symbol`` is the venue-native name (e.g. "BTC").
    The action still needs to be signed before submission.
    """
    order: dict[str, Any] = {
        "a": 0,  # asset index — must be resolved from metadata
        "b": is_buy,
        "p": str(price),
        "s": str(quantity),
        "r": reduce_only,
        "t": {"limit": {"tif": tif}},
    }
    if cloid is not None:
        order["c"] = cloid

    return {
        "type": "order",
        "orders": [order],
        "grouping": "na",
    }
