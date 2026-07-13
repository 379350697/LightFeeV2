"""Shared, immutable strategy-economics contracts.

Both strategies must use the same sign convention: a positive value improves
the trade and a positive cost field is deducted exactly once by
``build_edge_breakdown``.  Keeping the arithmetic here prevents sidecar,
entry, paper and reporting paths from silently drifting apart.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True, slots=True)
class EdgeBreakdown:
    """Complete expected/worst-case edge attribution in basis points."""

    gross_signal_edge_bps: float = 0.0
    funding_edge_bps: float = 0.0
    entry_cross_bps: float = 0.0
    expected_exit_cross_bps: float = 0.0
    entry_fee_bps: float = 0.0
    exit_fee_bps: float = 0.0
    entry_slippage_bps: float = 0.0
    exit_slippage_bps: float = 0.0
    adverse_selection_bps: float = 0.0
    capital_buffer_bps: float = 0.0
    execution_buffer_bps: float = 0.0
    venue_risk_haircut_bps: float = 0.0
    transfer_or_inventory_bias_bps: float = 0.0
    expected_net_edge_bps: float = 0.0
    worst_case_edge_bps: float = 0.0
    ranking_edge_bps: float = 0.0
    calculation_version: str = "v1_exact"
    model_epoch: str = "v1_exact"
    observed_at_ms: int = 0
    economics_complete: bool = False

    def candidate_fields(self) -> dict[str, float | int | str | bool]:
        """Dual-write fields kept by the legacy candidate/journal contract."""
        return {
            "gross_signal_edge_bps": self.gross_signal_edge_bps,
            "funding_edge_bps": self.funding_edge_bps,
            "entry_cross_bps": self.entry_cross_bps,
            "expected_exit_cross_bps": self.expected_exit_cross_bps,
            "entry_fee_bps": self.entry_fee_bps,
            "exit_fee_bps": self.exit_fee_bps,
            "entry_slippage_bps": self.entry_slippage_bps,
            "exit_slippage_bps": self.exit_slippage_bps,
            "adverse_selection_bps": self.adverse_selection_bps,
            "capital_buffer_bps": self.capital_buffer_bps,
            "execution_buffer_bps": self.execution_buffer_bps,
            "venue_risk_haircut_bps": self.venue_risk_haircut_bps,
            "transfer_or_inventory_bias_bps": self.transfer_or_inventory_bias_bps,
            "expected_net_edge_bps": self.expected_net_edge_bps,
            "worst_case_edge_bps": self.worst_case_edge_bps,
            "ranking_edge_bps": self.ranking_edge_bps,
            "calculation_version": self.calculation_version,
            "model_epoch": self.model_epoch,
            "economics_observed_at_ms": self.observed_at_ms,
            "economics_complete": self.economics_complete,
        }


def build_edge_breakdown(
    *,
    gross_signal_edge_bps: float = 0.0,
    funding_edge_bps: float = 0.0,
    worst_case_funding_edge_bps: float | None = None,
    entry_cross_bps: float = 0.0,
    expected_exit_cross_bps: float = 0.0,
    entry_fee_bps: float = 0.0,
    exit_fee_bps: float = 0.0,
    entry_slippage_bps: float = 0.0,
    exit_slippage_bps: float = 0.0,
    adverse_selection_bps: float = 0.0,
    capital_buffer_bps: float = 0.0,
    execution_buffer_bps: float = 0.0,
    venue_risk_haircut_bps: float = 0.0,
    transfer_or_inventory_bias_bps: float = 0.0,
    calculation_version: str = "v1_exact",
    model_epoch: str | None = None,
    observed_at_ms: int = 0,
    economics_complete: bool = False,
) -> EdgeBreakdown:
    """Construct the sole expected/worst-case economics formula.

    ``entry_cross_bps`` and ``expected_exit_cross_bps`` are signed realised
    cash-flow terms.  The remaining bps inputs model costs and are therefore
    always deducted.  ``worst_case_funding_edge_bps`` optionally replaces only
    expected funding carry in the conservative calculation, keeping expected
    and worst values inside one immutable contract.  Execution buffer only
    belongs to the worst case.
    """

    components = {
        "gross_signal_edge_bps": gross_signal_edge_bps,
        "funding_edge_bps": funding_edge_bps,
        "entry_cross_bps": entry_cross_bps,
        "expected_exit_cross_bps": expected_exit_cross_bps,
        "entry_fee_bps": entry_fee_bps,
        "exit_fee_bps": exit_fee_bps,
        "entry_slippage_bps": entry_slippage_bps,
        "exit_slippage_bps": exit_slippage_bps,
        "adverse_selection_bps": adverse_selection_bps,
        "capital_buffer_bps": capital_buffer_bps,
        "execution_buffer_bps": execution_buffer_bps,
        "venue_risk_haircut_bps": venue_risk_haircut_bps,
        "transfer_or_inventory_bias_bps": transfer_or_inventory_bias_bps,
    }
    normalized = {name: _finite_or_zero(value) for name, value in components.items()}
    worst_funding_input = (
        funding_edge_bps
        if worst_case_funding_edge_bps is None
        else worst_case_funding_edge_bps
    )
    normalized_worst_funding = _finite_or_zero(worst_funding_input)
    # IEEE-754 comparisons with NaN are false, which otherwise lets a
    # malformed live candidate pass both edge floors.  Keep the event payload
    # finite for diagnostics and revoke its live economics permission.
    values_complete = all(_is_finite_number(value) for value in components.values()) and _is_finite_number(
        worst_funding_input
    )
    expected = (
        normalized["gross_signal_edge_bps"]
        + normalized["funding_edge_bps"]
        + normalized["entry_cross_bps"]
        + normalized["expected_exit_cross_bps"]
        - normalized["entry_fee_bps"]
        - normalized["exit_fee_bps"]
        - normalized["entry_slippage_bps"]
        - normalized["exit_slippage_bps"]
        - normalized["adverse_selection_bps"]
        - normalized["capital_buffer_bps"]
        - normalized["venue_risk_haircut_bps"]
        + normalized["transfer_or_inventory_bias_bps"]
    )
    worst = (
        expected
        - normalized["funding_edge_bps"]
        + normalized_worst_funding
        - normalized["execution_buffer_bps"]
    )
    observed = _nonnegative_int(observed_at_ms)
    return EdgeBreakdown(
        **normalized,
        expected_net_edge_bps=expected,
        worst_case_edge_bps=worst,
        ranking_edge_bps=worst,
        calculation_version=str(calculation_version or "v1_exact"),
        model_epoch=str(model_epoch or calculation_version or "v1_exact"),
        observed_at_ms=observed,
        # This object is the admission contract shared by sidecar, live entry
        # and paper paths.  Do not let a truthy deserialisation artefact such
        # as ``"true"`` turn incomplete economics into live permission.
        economics_complete=(
            economics_complete is True and values_complete and observed > 0
        ),
    )


def _is_finite_number(value: object) -> bool:
    if isinstance(value, bool):
        return False
    try:
        return isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _finite_or_zero(value: object) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    return numeric if isfinite(numeric) else 0.0


def _nonnegative_int(value: object) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError, OverflowError):
        return 0


@dataclass(frozen=True, slots=True)
class FundingForecast:
    """Venue-normalised funding forecast with an explicit uncertainty range."""

    venue: str
    symbol: str
    next_funding_timestamp_ms: int
    funding_interval_ms: int
    quoted_rate_bps: float
    predicted_settled_rate_bps: float
    lower_bound_bps: float
    upper_bound_bps: float
    confidence: float
    sample_count: int
    source: str
    observed_at_ms: int

    @classmethod
    def from_quote(
        cls,
        *,
        venue: str,
        symbol: str,
        quoted_rate_bps: float,
        next_funding_timestamp_ms: int,
        funding_interval_ms: int,
        observed_at_ms: int,
        predicted_settled_rate_bps: float | None = None,
        uncertainty_haircut_bps: float = 0.0,
        sample_count: int = 0,
        min_samples: int = 0,
        source: str = "quoted_rate",
        confidence: float | None = None,
    ) -> "FundingForecast":
        quoted = _finite_or_zero(quoted_rate_bps)
        quoted_valid = _is_finite_number(quoted_rate_bps)
        prediction_supplied = predicted_settled_rate_bps is not None
        predicted_valid = (
            _is_finite_number(predicted_settled_rate_bps)
            if prediction_supplied
            else quoted_valid
        )
        predicted = (
            _finite_or_zero(predicted_settled_rate_bps)
            if prediction_supplied and predicted_valid
            else quoted
        )
        uncertainty_valid = _is_finite_number(uncertainty_haircut_bps)
        uncertainty = max(
            _finite_or_zero(uncertainty_haircut_bps) if uncertainty_valid else 0.0,
            0.0,
        )
        samples = _nonnegative_int(sample_count)
        required_samples = _nonnegative_int(min_samples)
        # A zero threshold is a caller that has not configured calibration;
        # it must never become accidental high confidence at cold start.
        calibrated = (
            quoted_valid
            and predicted_valid
            and uncertainty_valid
            and required_samples > 0
            and samples >= required_samples
        )
        if calibrated:
            supplied_confidence = (
                _finite_or_zero(confidence) if confidence is not None else 1.0
            )
            resolved_confidence = min(max(float(supplied_confidence), 0.0), 1.0)
        else:
            # The caller may pass an exchange/vendor confidence in the future,
            # but live gating is still governed by our own calibration sample
            # contract.  A cold-start or under-sampled forecast remains shadow
            # evidence only.
            resolved_confidence = 0.0
        return cls(
            venue=str(venue).lower(),
            symbol=str(symbol).upper(),
            next_funding_timestamp_ms=_nonnegative_int(next_funding_timestamp_ms),
            funding_interval_ms=_nonnegative_int(funding_interval_ms),
            quoted_rate_bps=quoted,
            predicted_settled_rate_bps=predicted,
            lower_bound_bps=predicted - uncertainty,
            upper_bound_bps=predicted + uncertainty,
            confidence=resolved_confidence,
            sample_count=samples,
            source=str(source or "quoted_rate"),
            observed_at_ms=_nonnegative_int(observed_at_ms),
        )


def conservative_funding_edge_bps(
    *, long_forecast: FundingForecast, short_forecast: FundingForecast
) -> tuple[float, float]:
    """Return expected and worst funding carry for long-low / short-high legs."""
    expected = (
        short_forecast.predicted_settled_rate_bps
        - long_forecast.predicted_settled_rate_bps
    )
    worst = short_forecast.lower_bound_bps - long_forecast.upper_bound_bps
    return expected, worst
