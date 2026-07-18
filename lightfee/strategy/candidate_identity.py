"""Stable economic identities for funding-entry candidates."""

from __future__ import annotations

import json
from hashlib import sha256
from math import isfinite
from typing import Mapping


def _number(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("identity numeric value must be a JSON number")
    try:
        parsed = float(value)
    except OverflowError:
        raise ValueError("identity numeric value is outside binary64") from None
    if not isfinite(parsed):
        raise ValueError("identity numeric value must be finite")
    # ``float.hex`` is a canonical, exact representation of the binary64
    # value.  Unlike a fixed significant-digit format it cannot collapse two
    # adjacent executable quantities or prices onto one revision.
    return parsed.hex()


def _integer(value: object) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def _text(value: object) -> str:
    return str(value or "").strip()


def _quote_contract(quote: object) -> dict[str, object]:
    return {
        "venue": _text(getattr(quote, "venue", "")).lower(),
        "symbol": _text(getattr(quote, "symbol", "")).upper(),
        # Observation/receipt clocks and funding sample IDs are deliberately
        # excluded.  The sample ID embeds observed_at_ms, so retaining it would
        # reintroduce wall-clock revision churn even after removing the explicit
        # timestamp fields.  Executable prices, settlement period, contract
        # rules, evidence source and derived economics remain authoritative.
        "funding_rate_source": _text(
            getattr(quote, "funding_rate_source", "")
        ),
        "funding_timestamp_ms": _integer(
            getattr(quote, "funding_timestamp_ms", 0)
        ),
        "funding_interval_ms": _integer(
            getattr(quote, "funding_interval_ms", 0)
        ),
        "bid": _number(getattr(quote, "bid", 0.0)),
        "ask": _number(getattr(quote, "ask", 0.0)),
        "underlying": _text(getattr(quote, "underlying", "")).upper(),
        "quote_currency": _text(
            getattr(quote, "quote_currency", "")
        ).upper(),
        "contract_type": _text(getattr(quote, "contract_type", "")).lower(),
        "contract_multiplier": _number(
            getattr(quote, "contract_multiplier", 0.0)
        ),
        "mark_index_source": _text(
            getattr(quote, "mark_index_source", "")
        ),
        "price_tick": _number(getattr(quote, "price_tick", 0.0)),
        "quantity_step_base": _number(
            getattr(quote, "quantity_step_base", 0.0)
        ),
        "min_quantity_base": _number(
            getattr(quote, "min_quantity_base", 0.0)
        ),
        "min_notional_quote": _number(
            getattr(quote, "min_notional_quote", 0.0)
        ),
        "min_notional_evidence_complete": (
            getattr(quote, "min_notional_evidence_complete", False) is True
        ),
        "venue_status": _text(getattr(quote, "venue_status", "")).lower(),
        "contract_normalization_complete": (
            getattr(quote, "contract_normalization_complete", False) is True
        ),
    }


def _digest(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()[:32]


def candidate_revision_id(
    *,
    pair_id: str,
    long_quote: object,
    short_quote: object,
    settlement_timestamps_ms: tuple[int, int, int, int],
    entry_route: str,
    exit_route: str,
    fee_evidence_fingerprint: str,
    fee_assurance_tier: str,
    model_epoch: str,
    economics: Mapping[str, object],
) -> str:
    """Hash every input that can change executable economics or legality."""
    try:
        return _digest(
            {
                "identity_version": "funding_candidate_revision_v5",
                "pair_id": _text(pair_id).lower(),
                "long_contract": _quote_contract(long_quote),
                "short_contract": _quote_contract(short_quote),
                "settlement_timestamps_ms": [
                    _integer(value) for value in settlement_timestamps_ms
                ],
                "execution_route": {
                    "entry": _text(entry_route).lower() or "taker_both",
                    "exit": _text(exit_route).lower() or "taker_both",
                },
                "fee_evidence_fingerprint": _text(fee_evidence_fingerprint),
                "fee_assurance_tier": _text(fee_assurance_tier).lower(),
                "model_epoch": _text(model_epoch),
                "economics": {
                    str(key): _number(value)
                    for key, value in sorted(economics.items())
                },
            },
        )
    except ValueError:
        # An empty identity is the established fail-closed signal at discovery
        # and snapshot admission.  Invalid evidence must never alias numeric 0.
        return ""


def opportunity_lease_id(
    *,
    pair_id: str,
    long_quote: object,
    short_quote: object,
    first_funding_timestamp_ms: int,
    second_funding_timestamp_ms: int,
    entry_route: str,
    exit_route: str,
    model_epoch: str,
) -> str:
    """Hash the stable opportunity epoch without quote/build wall clocks."""
    return _digest(
        {
            "identity_version": "funding_opportunity_lease_v2",
            "pair_id": _text(pair_id).lower(),
            "long": {
                "venue": _text(getattr(long_quote, "venue", "")).lower(),
                "symbol": _text(getattr(long_quote, "symbol", "")).upper(),
                "funding_timestamp_ms": _integer(
                    getattr(long_quote, "funding_timestamp_ms", 0)
                ),
                "funding_interval_ms": _integer(
                    getattr(long_quote, "funding_interval_ms", 0)
                ),
            },
            "short": {
                "venue": _text(getattr(short_quote, "venue", "")).lower(),
                "symbol": _text(getattr(short_quote, "symbol", "")).upper(),
                "funding_timestamp_ms": _integer(
                    getattr(short_quote, "funding_timestamp_ms", 0)
                ),
                "funding_interval_ms": _integer(
                    getattr(short_quote, "funding_interval_ms", 0)
                ),
            },
            "first_funding_timestamp_ms": _integer(first_funding_timestamp_ms),
            "second_funding_timestamp_ms": _integer(second_funding_timestamp_ms),
            "execution_route": {
                "entry": _text(entry_route).lower() or "taker_both",
                "exit": _text(exit_route).lower() or "taker_both",
            },
            "model_epoch": _text(model_epoch),
        }
    )


def final_candidate_revision_id(
    *,
    evidence_candidate_revision_id: str,
    quantity: float,
    long_price: float,
    short_price: float,
    entry_route: str,
    economics: Mapping[str, object],
) -> str:
    """Bind the immutable evidence revision to the exact submitted economics."""
    try:
        return _digest(
            {
                "identity_version": "funding_final_candidate_revision_v2",
                "evidence_candidate_revision_id": _text(
                    evidence_candidate_revision_id
                ),
                "quantity": _number(quantity),
                "long_price": _number(long_price),
                "short_price": _number(short_price),
                "entry_route": _text(entry_route).lower() or "taker_both",
                "economics": {
                    str(key): _number(value)
                    for key, value in sorted(economics.items())
                },
            },
        )
    except ValueError:
        return ""
