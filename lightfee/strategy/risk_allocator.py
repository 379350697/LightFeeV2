"""Common-base-quantity sizing for paired strategies.

The allocator deliberately accepts only evidence it can prove.  Missing
balance or depth information reduces the order rather than relaxing a risk
constraint.  Private-account admission remains the final runtime authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Iterable


@dataclass(frozen=True, slots=True)
class StrategyRiskAllocation:
    base_quantity: float
    reference_notional_quote: float
    long_leg_notional_quote: float
    short_leg_notional_quote: float
    constrained_by: tuple[str, ...]
    evidence_complete: bool


@dataclass(frozen=True, slots=True)
class StrategyRiskAdmission:
    """Portfolio-level admission result for one paired funding position."""

    allowed: bool
    reason: str
    reference_notional_quote: float
    gross_notional_quote: float
    projected_venue_exposure_quote: dict[str, float]
    projected_symbol_exposure_quote: float
    projected_venue_pair_exposure_quote: float
    projected_global_gross_exposure_quote: float
    projected_settlement_bucket_exposure_quote: float
    projected_correlation_group_exposure_quote: float
    projected_expected_shortfall_quote: float
    evidence_complete: bool


@dataclass(frozen=True, slots=True)
class ExpectedShortfallQuantityLimit:
    """Common-base cap implied by a paired-basis Expected Shortfall budget."""

    base_quantity: float
    reference_notional_quote: float
    maximum_reference_notional_quote: float
    constrained: bool
    evidence_complete: bool
    reason: str


class StrategyRiskAllocator:
    """Find a common base quantity under cap, depth and budget constraints."""

    def limit_base_quantity_by_expected_shortfall(
        self,
        *,
        long_entry_price: float,
        short_entry_price: float,
        current_base_quantity: float,
        expected_shortfall_bps: float,
        expected_shortfall_budget_quote: float,
    ) -> ExpectedShortfallQuantityLimit:
        """Reduce, never increase, a paired order to its ES capital budget.

        The budget is quote loss at the historical one-sided ES horizon.  It
        uses the matched reference notional (the more expensive executable
        leg), not the sum of legs, because the latter would double-count the
        same common-base basis exposure.
        """

        values = (
            long_entry_price,
            short_entry_price,
            current_base_quantity,
            expected_shortfall_bps,
            expected_shortfall_budget_quote,
        )
        if not all(_is_finite_number(value) for value in values):
            return ExpectedShortfallQuantityLimit(
                0.0, 0.0, 0.0, False, False, "nonfinite_expected_shortfall_input"
            )
        long_price = float(long_entry_price)
        short_price = float(short_entry_price)
        quantity = float(current_base_quantity)
        es_bps = float(expected_shortfall_bps)
        budget = float(expected_shortfall_budget_quote)
        reference_price = max(long_price, short_price)
        if reference_price <= 0.0 or quantity <= 0.0:
            return ExpectedShortfallQuantityLimit(
                0.0, 0.0, 0.0, False, False, "invalid_expected_shortfall_candidate"
            )
        if es_bps <= 0.0 or budget <= 0.0:
            return ExpectedShortfallQuantityLimit(
                0.0, 0.0, 0.0, False, False, "missing_expected_shortfall_model"
            )
        maximum_reference_notional = budget * 10_000.0 / es_bps
        capped_quantity = min(quantity, maximum_reference_notional / reference_price)
        reference_notional = capped_quantity * reference_price
        return ExpectedShortfallQuantityLimit(
            base_quantity=max(capped_quantity, 0.0),
            reference_notional_quote=max(reference_notional, 0.0),
            maximum_reference_notional_quote=maximum_reference_notional,
            constrained=capped_quantity + 1e-12 < quantity,
            evidence_complete=True,
            reason="",
        )

    def allocate(
        self,
        *,
        long_entry_price: float,
        short_entry_price: float,
        long_max_quantity: float,
        short_max_quantity: float,
        configured_notional_cap_quote: float,
        venue_notional_cap_quote: float = 0.0,
        symbol_risk_budget_quote: float = 0.0,
        venue_pair_risk_budget_quote: float = 0.0,
        global_risk_budget_quote: float = 0.0,
        available_margin_quote: float | None = None,
        long_available_margin_quote: float | None = None,
        short_available_margin_quote: float | None = None,
        target_leverage: float = 1.0,
        health_buffer_ratio: float = 1.0,
        fallback_notional_quote: float = 0.0,
    ) -> StrategyRiskAllocation:
        # A risk allocator is an admission boundary.  ``max(nan, 0.0)`` and
        # comparisons such as ``nan > 0`` are both unsafe here: they can make
        # a limit disappear or produce a NaN order size which downstream
        # venues interpret differently.  Treat every non-finite input as
        # missing evidence and return zero size.
        values = (
            long_entry_price,
            short_entry_price,
            long_max_quantity,
            short_max_quantity,
            configured_notional_cap_quote,
            venue_notional_cap_quote,
            symbol_risk_budget_quote,
            venue_pair_risk_budget_quote,
            global_risk_budget_quote,
            target_leverage,
            health_buffer_ratio,
            fallback_notional_quote,
        )
        margin_inputs = (
            available_margin_quote,
            long_available_margin_quote,
            short_available_margin_quote,
        )
        values = values + tuple(
            value for value in margin_inputs if value is not None
        )
        if not all(_is_finite_number(value) for value in values):
            return StrategyRiskAllocation(
                0.0, 0.0, 0.0, 0.0, ("nonfinite_risk_input",), False
            )

        long_price = max(float(long_entry_price or 0.0), 0.0)
        short_price = max(float(short_entry_price or 0.0), 0.0)
        reference_price = (long_price + short_price) / 2.0
        if reference_price <= 0.0:
            return StrategyRiskAllocation(0.0, 0.0, 0.0, 0.0, ("invalid_price",), False)
        if float(target_leverage or 0.0) <= 0.0:
            return StrategyRiskAllocation(
                0.0, 0.0, 0.0, 0.0, ("invalid_target_leverage",), False
            )

        caps: list[tuple[str, float]] = []
        for name, value in (
            ("configured_cap", configured_notional_cap_quote),
            ("venue_cap", venue_notional_cap_quote),
            ("symbol_budget", symbol_risk_budget_quote),
            ("venue_pair_budget", venue_pair_risk_budget_quote),
            ("global_budget", global_risk_budget_quote),
        ):
            numeric = float(value or 0.0)
            if numeric > 0.0:
                caps.append((name, numeric))

        # ``available_margin_quote`` is retained for the public-sidecar
        # fallback contract.  A live paired order must instead provide one
        # free-collateral fact per leg: the same quote cap on both venues is
        # not an economically meaningful margin proof.
        per_leg_margin_requested = (
            long_available_margin_quote is not None
            or short_available_margin_quote is not None
        )
        evidence_complete = (
            long_available_margin_quote is not None
            and short_available_margin_quote is not None
            if per_leg_margin_requested
            else available_margin_quote is not None
        )
        health_ratio = max(min(float(health_buffer_ratio or 0.0), 1.0), 0.0)
        leverage = max(float(target_leverage or 0.0), 1.0)
        if per_leg_margin_requested:
            # A paired order is admissible only up to the smaller *base*
            # quantity funded by either leg.  This is intentionally not an
            # averaged-notional cap: unequal prices make such an average
            # capable of over-sizing one exchange.
            margin_quantity_limits: list[tuple[str, float]] = []
            for name, available, price in (
                ("long_margin_health", long_available_margin_quote, long_price),
                ("short_margin_health", short_available_margin_quote, short_price),
            ):
                if available is None:
                    continue
                margin_quantity_limits.append(
                    (
                        name,
                        max(float(available), 0.0) * health_ratio * leverage / price,
                    )
                )
            if not evidence_complete:
                # Missing private evidence never relaxes the known leg's
                # cap.  The configured small notional fallback additionally
                # constrains the unknown leg; a zero fallback fails closed.
                fallback_quantity = (
                    float(fallback_notional_quote) / reference_price
                    if fallback_notional_quote > 0.0
                    else 0.0
                )
                margin_quantity_limits.append(
                    ("missing_margin_fallback", fallback_quantity)
                )
        else:
            margin_quantity_limits = []
        if available_margin_quote is not None and not per_leg_margin_requested:
            margin_cap = max(float(available_margin_quote), 0.0) * max(
                health_ratio,
                0.0,
            )
            caps.append(("margin_health", margin_cap))
        elif not per_leg_margin_requested:
            # No private margin fact is not permission to use the public
            # notional cap.  A configured small fallback is the only safe
            # sizing input; a zero/absent fallback explicitly means no entry.
            if fallback_notional_quote > 0.0:
                caps.append(("missing_margin_fallback", float(fallback_notional_quote)))
            else:
                return StrategyRiskAllocation(
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    ("missing_margin_fallback",),
                    False,
                )

        cap_name, cap = min(caps, key=lambda item: item[1]) if caps else ("no_cap", 0.0)
        if cap <= 0.0:
            return StrategyRiskAllocation(0.0, 0.0, 0.0, 0.0, (cap_name,), evidence_complete)

        quantity_limits = [
            ("notional_cap", cap / reference_price),
            ("long_depth", max(float(long_max_quantity or 0.0), 0.0)),
            ("short_depth", max(float(short_max_quantity or 0.0), 0.0)),
            *margin_quantity_limits,
        ]
        limiting_quantity = min(value for _, value in quantity_limits)
        constrained_by = tuple(
            name
            for name, value in quantity_limits
            if abs(value - limiting_quantity) <= 1e-12
        )
        if cap_name and cap_name != "no_cap":
            constrained_by = (cap_name,) + constrained_by
        base_quantity = max(limiting_quantity, 0.0)
        return StrategyRiskAllocation(
            base_quantity=base_quantity,
            reference_notional_quote=base_quantity * reference_price,
            long_leg_notional_quote=base_quantity * long_price,
            short_leg_notional_quote=base_quantity * short_price,
            constrained_by=constrained_by,
            evidence_complete=evidence_complete,
        )

    def assess_portfolio_admission(
        self,
        *,
        open_positions: Iterable[object],
        symbol: str,
        long_venue: str,
        short_venue: str,
        long_entry_price: float,
        short_entry_price: float,
        base_quantity: float,
        first_funding_timestamp_ms: int,
        max_concurrent_positions: int,
        max_single_venue_exposure_quote: float,
        max_symbol_exposure_quote: float,
        max_concurrent_venue_pairs: int,
        max_venue_pair_exposure_quote: float,
        max_global_gross_exposure_quote: float,
        max_settlement_bucket_exposure_quote: float,
        settlement_crowding_bucket_ms: int,
        max_correlation_group_exposure_quote: float,
        correlation_group_by_symbol: dict[str, str],
        expected_shortfall_bps: float,
        expected_shortfall_budget_quote: float,
        max_concurrent_positions_per_venue: int = 0,
        max_concurrent_positions_per_symbol: int = 0,
        max_concurrent_positions_per_venue_pair: int = 0,
    ) -> StrategyRiskAdmission:
        """Check a pair against live portfolio concentration limits.

        The method deliberately accepts position objects structurally so it can
        consume recovered V1 ``OpenPosition`` state without a parallel risk
        store. Per-venue exposure is leg notional; symbol/pair exposure is the
        matched reference notional; global gross uses both legs. This avoids
        treating a delta-neutral pair as zero capital or zero venue risk.
        """
        policy_values = (
            long_entry_price,
            short_entry_price,
            base_quantity,
            max_single_venue_exposure_quote,
            max_symbol_exposure_quote,
            max_venue_pair_exposure_quote,
            max_global_gross_exposure_quote,
            max_settlement_bucket_exposure_quote,
            max_correlation_group_exposure_quote,
            expected_shortfall_bps,
            expected_shortfall_budget_quote,
        )
        if not all(_is_finite_number(value) for value in policy_values):
            return _risk_admission_block("nonfinite_risk_input")

        long_price = float(long_entry_price or 0.0)
        short_price = float(short_entry_price or 0.0)
        quantity = float(base_quantity or 0.0)
        if (
            quantity <= 0.0
            or long_price <= 0.0
            or short_price <= 0.0
            or not all(isfinite(value) for value in (quantity, long_price, short_price))
        ):
            return _risk_admission_block("invalid_candidate_risk_inputs")

        symbol_key = str(symbol or "").upper()
        long_venue_key = _venue_key(long_venue)
        short_venue_key = _venue_key(short_venue)
        if not symbol_key or not long_venue_key or not short_venue_key:
            return _risk_admission_block("missing_candidate_risk_identity")

        candidate_long = quantity * long_price
        candidate_short = quantity * short_price
        reference = max(candidate_long, candidate_short)
        gross = candidate_long + candidate_short
        pair_key = tuple(sorted((long_venue_key, short_venue_key)))
        correlation_groups = {
            str(key).upper(): str(value).strip()
            for key, value in correlation_group_by_symbol.items()
            if str(key).strip() and str(value).strip()
        }
        correlation_group = correlation_groups.get(symbol_key, symbol_key)

        venue_exposure: dict[str, float] = {}
        symbol_exposure: dict[str, float] = {}
        pair_exposure: dict[tuple[str, str], float] = {}
        venue_position_count: dict[str, int] = {}
        symbol_position_count: dict[str, int] = {}
        pair_position_count: dict[tuple[str, str], int] = {}
        settlement_exposure: dict[int, float] = {}
        correlation_exposure: dict[str, float] = {}
        global_gross = 0.0
        expected_shortfall = 0.0
        positions = list(open_positions)
        expected_shortfall_budget_enabled = expected_shortfall_budget_quote > 0.0
        if expected_shortfall_budget_enabled and expected_shortfall_bps <= 0.0:
            return _risk_admission_block("missing_expected_shortfall_model")
        enhanced_limits_enabled = any(
            float(value or 0.0) > 0.0
            for value in (
                max_venue_pair_exposure_quote,
                max_global_gross_exposure_quote,
                max_settlement_bucket_exposure_quote,
                max_correlation_group_exposure_quote,
                expected_shortfall_budget_quote,
            )
        )
        bucket_ms = _nonnegative_int(settlement_crowding_bucket_ms)
        for position in positions:
            exposure = _position_exposure(position)
            if exposure is None:
                if enhanced_limits_enabled:
                    return _risk_admission_block("incomplete_open_position_risk_evidence")
                continue
            position_symbol, position_long_venue, position_short_venue, long_notional, short_notional, position_first_ts = exposure
            position_reference = max(long_notional, short_notional)
            position_gross = long_notional + short_notional
            venue_exposure[position_long_venue] = (
                venue_exposure.get(position_long_venue, 0.0) + long_notional
            )
            venue_exposure[position_short_venue] = (
                venue_exposure.get(position_short_venue, 0.0) + short_notional
            )
            venue_position_count[position_long_venue] = (
                venue_position_count.get(position_long_venue, 0) + 1
            )
            venue_position_count[position_short_venue] = (
                venue_position_count.get(position_short_venue, 0) + 1
            )
            symbol_exposure[position_symbol] = (
                symbol_exposure.get(position_symbol, 0.0) + position_reference
            )
            symbol_position_count[position_symbol] = (
                symbol_position_count.get(position_symbol, 0) + 1
            )
            position_pair = tuple(sorted((position_long_venue, position_short_venue)))
            pair_exposure[position_pair] = (
                pair_exposure.get(position_pair, 0.0) + position_reference
            )
            pair_position_count[position_pair] = (
                pair_position_count.get(position_pair, 0) + 1
            )
            global_gross += position_gross
            position_group = correlation_groups.get(position_symbol, position_symbol)
            correlation_exposure[position_group] = (
                correlation_exposure.get(position_group, 0.0) + position_reference
            )
            if bucket_ms > 0 and position_first_ts > 0:
                bucket = position_first_ts // bucket_ms
                settlement_exposure[bucket] = (
                    settlement_exposure.get(bucket, 0.0) + position_reference
                )
            elif max_settlement_bucket_exposure_quote > 0.0:
                return _risk_admission_block("incomplete_open_position_settlement_time")
            if expected_shortfall_budget_enabled:
                # ES is an entry-time property of the existing position.  It
                # is not valid to substitute the new candidate's volatility
                # estimate here: doing so can materially understate a prior,
                # more volatile position and admit excess portfolio risk.
                position_es_bps = _position_expected_shortfall_bps(position)
                if position_es_bps is None:
                    return _risk_admission_block(
                        "incomplete_open_position_expected_shortfall_evidence"
                    )
                expected_shortfall += position_reference * position_es_bps / 10_000.0

        projected_venue = dict(venue_exposure)
        projected_venue[long_venue_key] = projected_venue.get(long_venue_key, 0.0) + candidate_long
        projected_venue[short_venue_key] = projected_venue.get(short_venue_key, 0.0) + candidate_short
        projected_symbol = symbol_exposure.get(symbol_key, 0.0) + reference
        projected_pair = pair_exposure.get(pair_key, 0.0) + reference
        projected_global_gross = global_gross + gross
        projected_group = correlation_exposure.get(correlation_group, 0.0) + reference
        projected_es = expected_shortfall + reference * max(float(expected_shortfall_bps or 0.0), 0.0) / 10_000.0
        settlement_bucket_exposure = 0.0
        first_timestamp = _nonnegative_int(first_funding_timestamp_ms)
        if max_settlement_bucket_exposure_quote > 0.0:
            if bucket_ms <= 0 or first_timestamp <= 0:
                return _risk_admission_block("missing_candidate_settlement_time")
            bucket = first_timestamp // bucket_ms
            settlement_bucket_exposure = settlement_exposure.get(bucket, 0.0) + reference

        projected_pair_count = len(pair_exposure | {pair_key: projected_pair})
        admission = StrategyRiskAdmission(
            allowed=True,
            reason="",
            reference_notional_quote=reference,
            gross_notional_quote=gross,
            projected_venue_exposure_quote=projected_venue,
            projected_symbol_exposure_quote=projected_symbol,
            projected_venue_pair_exposure_quote=projected_pair,
            projected_global_gross_exposure_quote=projected_global_gross,
            projected_settlement_bucket_exposure_quote=settlement_bucket_exposure,
            projected_correlation_group_exposure_quote=projected_group,
            projected_expected_shortfall_quote=projected_es,
            evidence_complete=True,
        )
        if max_concurrent_positions > 0 and len(positions) >= max_concurrent_positions:
            return _risk_admission_with_reason(admission, "max_concurrent_positions")
        if max_concurrent_positions_per_venue > 0 and any(
            venue_position_count.get(venue, 0) + 1
            > max_concurrent_positions_per_venue
            for venue in (long_venue_key, short_venue_key)
        ):
            return _risk_admission_with_reason(
                admission, "max_concurrent_positions_per_venue"
            )
        if (
            max_concurrent_positions_per_symbol > 0
            and symbol_position_count.get(symbol_key, 0) + 1
            > max_concurrent_positions_per_symbol
        ):
            return _risk_admission_with_reason(
                admission, "max_concurrent_positions_per_symbol"
            )
        if (
            max_concurrent_positions_per_venue_pair > 0
            and pair_position_count.get(pair_key, 0) + 1
            > max_concurrent_positions_per_venue_pair
        ):
            return _risk_admission_with_reason(
                admission, "max_concurrent_positions_per_venue_pair"
            )
        if max_concurrent_venue_pairs > 0 and projected_pair_count > max_concurrent_venue_pairs:
            return _risk_admission_with_reason(admission, "max_concurrent_venue_pairs")
        if max_single_venue_exposure_quote > 0.0 and any(
            value > max_single_venue_exposure_quote
            for value in projected_venue.values()
        ):
            return _risk_admission_with_reason(admission, "max_single_venue_exposure")
        if max_symbol_exposure_quote > 0.0 and projected_symbol > max_symbol_exposure_quote:
            return _risk_admission_with_reason(admission, "max_symbol_exposure")
        if max_venue_pair_exposure_quote > 0.0 and projected_pair > max_venue_pair_exposure_quote:
            return _risk_admission_with_reason(admission, "max_venue_pair_exposure")
        if max_global_gross_exposure_quote > 0.0 and projected_global_gross > max_global_gross_exposure_quote:
            return _risk_admission_with_reason(admission, "max_global_gross_exposure")
        if (
            max_settlement_bucket_exposure_quote > 0.0
            and settlement_bucket_exposure > max_settlement_bucket_exposure_quote
        ):
            return _risk_admission_with_reason(admission, "max_settlement_bucket_exposure")
        if (
            max_correlation_group_exposure_quote > 0.0
            and projected_group > max_correlation_group_exposure_quote
        ):
            return _risk_admission_with_reason(admission, "max_correlation_group_exposure")
        if expected_shortfall_budget_enabled:
            if projected_es > expected_shortfall_budget_quote:
                return _risk_admission_with_reason(admission, "expected_shortfall_budget")
        return admission


def _venue_key(value: object) -> str:
    return str(getattr(value, "value", value) or "").lower()


def _is_finite_number(value: object) -> bool:
    try:
        return isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _nonnegative_int(value: object) -> int:
    try:
        numeric = float(value or 0)
    except (TypeError, ValueError):
        return 0
    if not isfinite(numeric):
        return 0
    return max(int(numeric), 0)


def _position_exposure(
    position: object,
) -> tuple[str, str, str, float, float, int] | None:
    symbol = str(getattr(position, "symbol", "") or "").upper()
    long_venue = _venue_key(getattr(position, "long_venue", ""))
    short_venue = _venue_key(getattr(position, "short_venue", ""))
    long_quantity = _finite_float(getattr(position, "long_quantity", 0.0))
    short_quantity = _finite_float(getattr(position, "short_quantity", 0.0))
    long_price = _finite_float(getattr(position, "long_entry_price", 0.0))
    short_price = _finite_float(getattr(position, "short_entry_price", 0.0))
    if None in (long_quantity, short_quantity, long_price, short_price):
        return None
    long_quantity = abs(long_quantity)
    short_quantity = abs(short_quantity)
    values = (long_quantity, short_quantity, long_price, short_price)
    if (
        not symbol
        or not long_venue
        or not short_venue
        or any(value <= 0.0 or not isfinite(value) for value in values)
    ):
        return None
    return (
        symbol,
        long_venue,
        short_venue,
        long_quantity * long_price,
        short_quantity * short_price,
        _nonnegative_int(
            getattr(
                position,
                "first_funding_timestamp_ms",
                getattr(position, "funding_timestamp_ms", 0),
            )
        ),
    )


def _position_expected_shortfall_bps(position: object) -> float | None:
    """Return the immutable entry ES model evidence for an open position."""
    value = _finite_float(getattr(position, "expected_shortfall_bps_entry", None))
    return value if value is not None and value > 0.0 else None


def _finite_float(value: object) -> float | None:
    try:
        numeric = float(value or 0.0)
    except (TypeError, ValueError):
        return None
    return numeric if isfinite(numeric) else None


def _risk_admission_block(reason: str) -> StrategyRiskAdmission:
    return StrategyRiskAdmission(
        allowed=False,
        reason=reason,
        reference_notional_quote=0.0,
        gross_notional_quote=0.0,
        projected_venue_exposure_quote={},
        projected_symbol_exposure_quote=0.0,
        projected_venue_pair_exposure_quote=0.0,
        projected_global_gross_exposure_quote=0.0,
        projected_settlement_bucket_exposure_quote=0.0,
        projected_correlation_group_exposure_quote=0.0,
        projected_expected_shortfall_quote=0.0,
        evidence_complete=False,
    )


def _risk_admission_with_reason(
    admission: StrategyRiskAdmission,
    reason: str,
) -> StrategyRiskAdmission:
    return StrategyRiskAdmission(
        allowed=False,
        reason=reason,
        reference_notional_quote=admission.reference_notional_quote,
        gross_notional_quote=admission.gross_notional_quote,
        projected_venue_exposure_quote=admission.projected_venue_exposure_quote,
        projected_symbol_exposure_quote=admission.projected_symbol_exposure_quote,
        projected_venue_pair_exposure_quote=admission.projected_venue_pair_exposure_quote,
        projected_global_gross_exposure_quote=admission.projected_global_gross_exposure_quote,
        projected_settlement_bucket_exposure_quote=admission.projected_settlement_bucket_exposure_quote,
        projected_correlation_group_exposure_quote=admission.projected_correlation_group_exposure_quote,
        projected_expected_shortfall_quote=admission.projected_expected_shortfall_quote,
        evidence_complete=admission.evidence_complete,
    )
