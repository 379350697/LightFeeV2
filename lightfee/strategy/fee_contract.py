"""Deterministic fee derivation shared by snapshot and final entry gates."""

from __future__ import annotations

from math import isfinite


def derive_candidate_stage_fee_bps(
    candidate: object,
    stage: str,
    *,
    maker_leg_override: str | None = None,
) -> tuple[float | None, str]:
    """Preserve a conservative stage fee above the authenticated taker floor."""
    if stage not in {"entry", "exit"}:
        return None, "invalid_fee_stage"
    long_fee = _finite_nonnegative(_value(candidate, "long_taker_fee_bps", None))
    short_fee = _finite_nonnegative(_value(candidate, "short_taker_fee_bps", None))
    if long_fee is None or short_fee is None:
        return None, "invalid_taker_fee_evidence"
    maker_leg = str(
        _value(candidate, f"{stage}_maker_leg", "")
        if maker_leg_override is None
        else maker_leg_override
        or ""
    ).lower()
    if maker_leg and maker_leg not in {"long", "short"}:
        return None, f"invalid_{stage}_maker_leg"
    # Serialized provenance cannot prove its own HMAC, so the two-taker cost
    # remains the minimum admissible stage fee.  A larger candidate-stage fee
    # is conservative (it removes alpha) and must be preserved end-to-end;
    # otherwise a legal maker>taker schedule passes pairing but is rejected by
    # the loader or silently repriced downward at final revalidation.
    taker_floor = long_fee + short_fee
    asserted_stage_fee = _finite_number(
        _value(candidate, f"{stage}_fee_bps", None)
    )
    if asserted_stage_fee is not None and asserted_stage_fee >= taker_floor - 1e-12:
        return asserted_stage_fee, ""
    return taker_floor, ""


def _value(candidate: object, name: str, default: object) -> object:
    if isinstance(candidate, dict):
        return candidate.get(name, default)
    return getattr(candidate, name, default)


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    parsed = float(value)
    return parsed if isfinite(parsed) else None


def _finite_nonnegative(value: object) -> float | None:
    parsed = _finite_number(value)
    return parsed if parsed is not None and parsed >= 0.0 else None
