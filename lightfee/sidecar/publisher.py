"""Atomic snapshot publisher for sidecar output."""

from __future__ import annotations

import json
from math import isclose, isfinite
import os
import tempfile
from pathlib import Path

from lightfee.sidecar.snapshot import QuoteSnapshot, SidecarSnapshot
from lightfee.strategy.economics import build_edge_breakdown


_V3_ECONOMICS_NUMERIC_FIELDS = (
    "gross_signal_edge_bps",
    "funding_edge_bps",
    "entry_cross_bps",
    "expected_exit_cross_bps",
    "entry_fee_bps",
    "exit_fee_bps",
    "entry_slippage_bps",
    "exit_slippage_bps",
    "adverse_selection_bps",
    "capital_buffer_bps",
    "execution_buffer_bps",
    "venue_risk_haircut_bps",
    "transfer_or_inventory_bias_bps",
    "expected_net_edge_bps",
    "worst_case_edge_bps",
    "ranking_edge_bps",
    "forecast_worst_funding_edge_bps",
    "long_taker_fee_bps",
    "short_taker_fee_bps",
)

_V3_LIFECYCLE_NUMERIC_FIELDS = (
    "first_stage_funding_edge_bps",
    "first_stage_expected_edge_bps",
    "first_stage_worst_case_edge_bps",
    "second_stage_incremental_funding_edge_bps",
    "second_stage_worst_case_funding_edge_bps",
    "stagger_gap_ms",
)


def _v3_economics_contract_reason(
    candidate: dict,
    *,
    quotes_raw: object,
) -> str:
    """Return a fail-closed reason for a claimed-complete V3 candidate.

    JSON defaults are convenient for diagnostics but dangerous at a live
    admission boundary: a truncated V3 record used to inherit zero sizing and
    cost fields from ``CandidateInput``.  Verify both the raw presence and the
    shared formula and the candidate's two contract-normalisation proofs before
    constructing the dataclass, so no live caller can confuse a partially
    decoded or cross-contract candidate with a valid economics assertion.
    """
    required = (
        *_V3_ECONOMICS_NUMERIC_FIELDS,
        *_V3_LIFECYCLE_NUMERIC_FIELDS,
        "entry_notional_quote",
        "entry_target_quantity",
        "entry_max_executable_quantity",
        "economics_observed_at_ms",
        "calculation_version",
        "model_epoch",
        "taker_fee_evidence_complete",
        "forecast_distribution_stable",
        "forecast_stability_reason",
    )
    missing = next((name for name in required if name not in candidate), "")
    if missing:
        return f"missing_v3_economics_field:{missing}"
    if candidate.get("taker_fee_evidence_complete") is not True:
        return "missing_taker_fee_evidence"
    if not isinstance(candidate.get("forecast_distribution_stable"), bool):
        return "invalid_v3_economics_field:forecast_distribution_stable"
    stability_reason = candidate.get("forecast_stability_reason")
    if not isinstance(stability_reason, str) or not stability_reason.strip():
        return "invalid_v3_economics_field:forecast_stability_reason"

    numeric_fields = (
        *_V3_ECONOMICS_NUMERIC_FIELDS,
        *_V3_LIFECYCLE_NUMERIC_FIELDS,
        "entry_notional_quote",
        "entry_target_quantity",
        "entry_max_executable_quantity",
    )
    values: dict[str, float] = {}
    for name in numeric_fields:
        if isinstance(candidate[name], bool):
            return f"invalid_v3_economics_field:{name}"
        try:
            value = float(candidate[name])
        except (TypeError, ValueError, OverflowError):
            return f"invalid_v3_economics_field:{name}"
        if not isfinite(value):
            return f"invalid_v3_economics_field:{name}"
        values[name] = value
    # Transfer/inventory scoring was deliberately removed from the public
    # sidecar: it has neither a balance source nor position-allocation proof.
    # Keeping this field in the schema preserves historical diagnostics, but a
    # snapshot may not reintroduce an unproved alpha by self-reporting a bias.
    # A future private live-admission implementation needs a separate,
    # evidence-carrying contract rather than weakening this parser boundary.
    if not isclose(
        values["transfer_or_inventory_bias_bps"],
        0.0,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        return "unproved_transfer_or_inventory_bias"
    if any(
        values[name] <= 0.0
        for name in (
            "entry_notional_quote",
            "entry_target_quantity",
            "entry_max_executable_quantity",
        )
    ):
        return "invalid_v3_unified_sizing"
    # Fee fields remain signed: a passive maker leg can have a verified rebate
    # and pairing records it as a real cash flow.  Reserves and execution-risk
    # terms are not cash flows; a negative value there is untrusted alpha.
    for name in (
        "entry_slippage_bps",
        "exit_slippage_bps",
        "adverse_selection_bps",
        "capital_buffer_bps",
        "execution_buffer_bps",
        "venue_risk_haircut_bps",
    ):
        if values[name] < 0.0:
            return f"invalid_v3_economics_cost_sign:{name}"
    # A negative fee is a maker rebate, not a generic discount.  The candidate
    # must therefore retain the selected passive leg that makes the rebate
    # executable.  Without that proof an untrusted V3 payload could turn a
    # missing fee schedule into positive alpha simply by sending ``-x``.
    for fee_field, maker_leg_field in (
        ("entry_fee_bps", "entry_maker_leg"),
        ("exit_fee_bps", "exit_maker_leg"),
    ):
        if values[fee_field] < 0.0 and str(
            candidate.get(maker_leg_field, "") or ""
        ).lower() not in {"long", "short"}:
            return f"invalid_v3_signed_fee_proof:{maker_leg_field}"
    if isinstance(candidate["economics_observed_at_ms"], bool):
        return "invalid_v3_economics_observed_at_ms"
    try:
        observed_at_ms = int(candidate["economics_observed_at_ms"])
    except (TypeError, ValueError, OverflowError):
        return "invalid_v3_economics_observed_at_ms"
    if observed_at_ms <= 0:
        return "invalid_v3_economics_observed_at_ms"
    if not str(candidate["calculation_version"] or ""):
        return "missing_v3_calculation_version"
    if not str(candidate["model_epoch"] or ""):
        return "missing_v3_model_epoch"

    edge = build_edge_breakdown(
        gross_signal_edge_bps=values["gross_signal_edge_bps"],
        funding_edge_bps=values["funding_edge_bps"],
        worst_case_funding_edge_bps=values["forecast_worst_funding_edge_bps"],
        entry_cross_bps=values["entry_cross_bps"],
        expected_exit_cross_bps=values["expected_exit_cross_bps"],
        entry_fee_bps=values["entry_fee_bps"],
        exit_fee_bps=values["exit_fee_bps"],
        entry_slippage_bps=values["entry_slippage_bps"],
        exit_slippage_bps=values["exit_slippage_bps"],
        adverse_selection_bps=values["adverse_selection_bps"],
        capital_buffer_bps=values["capital_buffer_bps"],
        execution_buffer_bps=values["execution_buffer_bps"],
        venue_risk_haircut_bps=values["venue_risk_haircut_bps"],
        transfer_or_inventory_bias_bps=values["transfer_or_inventory_bias_bps"],
        calculation_version=str(candidate["calculation_version"]),
        model_epoch=str(candidate["model_epoch"]),
        observed_at_ms=observed_at_ms,
        economics_complete=True,
    )
    if not edge.economics_complete:
        return "invalid_v3_economics_cost_sign"
    for field, computed in (
        ("expected_net_edge_bps", edge.expected_net_edge_bps),
        ("worst_case_edge_bps", edge.worst_case_edge_bps),
        ("ranking_edge_bps", edge.ranking_edge_bps),
    ):
        if not isclose(values[field], computed, rel_tol=0.0, abs_tol=1e-9):
            return f"v3_edge_formula_mismatch:{field}"
    try:
        expected_edge = float(candidate.get("expected_edge_bps", 0.0) or 0.0)
    except (TypeError, ValueError, OverflowError):
        return "invalid_v3_economics_field:expected_edge_bps"
    if not isfinite(expected_edge):
        return "invalid_v3_economics_field:expected_edge_bps"
    if not isclose(
        expected_edge,
        edge.expected_net_edge_bps,
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        return "v3_edge_formula_mismatch:expected_edge_bps"
    return _v3_candidate_contract_reason(candidate, quotes_raw)


def _v3_candidate_contract_reason(candidate: dict, quotes_raw: object) -> str:
    """Verify that a V3 candidate is backed by compatible quote contracts.

    Candidate economics are serialised separately from quote metadata.  A
    formula-correct but replayed/tampered candidate must not be allowed to
    claim a common base quantity if its two corresponding quote records cannot
    still prove the same linear economic unit.  The sidecar pair builder does
    the equivalent check when it creates a candidate; this parser check binds
    that proof to the live snapshot trust boundary.
    """
    if not isinstance(quotes_raw, dict):
        return "missing_v3_contract_evidence:quotes"
    symbol = str(candidate.get("symbol", "") or "").upper()
    long_venue = str(candidate.get("long_venue", "") or "").lower()
    short_venue = str(candidate.get("short_venue", "") or "").lower()
    if not symbol or not long_venue or not short_venue or long_venue == short_venue:
        return "invalid_v3_contract_evidence:candidate_identity"
    long_quote, long_quote_reason = _v3_quote_for_contract_evidence(
        quotes_raw,
        long_venue,
        symbol,
    )
    short_quote, short_quote_reason = _v3_quote_for_contract_evidence(
        quotes_raw,
        short_venue,
        symbol,
    )
    if long_quote is None:
        return f"{long_quote_reason}_v3_contract_evidence:long_quote"
    if short_quote is None:
        return f"{short_quote_reason}_v3_contract_evidence:short_quote"
    long_contract = _v3_normalised_contract_fields(long_quote)
    short_contract = _v3_normalised_contract_fields(short_quote)
    if long_contract is None:
        return "invalid_v3_contract_evidence:long_quote"
    if short_contract is None:
        return "invalid_v3_contract_evidence:short_quote"
    (
        long_underlying,
        long_quote_currency,
        long_multiplier,
    ) = long_contract
    (
        short_underlying,
        short_quote_currency,
        short_multiplier,
    ) = short_contract
    if long_underlying != short_underlying:
        return "v3_contract_evidence:underlying_mismatch"
    if long_quote_currency != short_quote_currency:
        return "v3_contract_evidence:quote_currency_mismatch"
    if not isclose(long_multiplier, short_multiplier, rel_tol=1e-12, abs_tol=0.0):
        return "v3_contract_evidence:multiplier_mismatch"
    return ""


def _v3_quote_for_contract_evidence(
    quotes_raw: dict,
    venue: str,
    symbol: str,
) -> tuple[dict | None, str]:
    """Find a quote by its own identity, not only by a mutable dict key."""
    match: dict | None = None
    for raw in quotes_raw.values():
        if not isinstance(raw, dict):
            continue
        raw_venue = str(raw.get("venue", "") or "").lower()
        raw_symbol = str(raw.get("symbol", "") or "").upper()
        if raw_venue == venue and raw_symbol == symbol:
            # Two differently keyed records that claim the same market make
            # the source ambiguous.  Choosing the first one would let a
            # tampered valid-looking quote vouch for another record's
            # candidate, so reject the entire proof rather than relying on
            # JSON insertion order.
            if match is not None:
                return None, "ambiguous"
            match = raw
    return (match, "") if match is not None else (None, "missing")


def _v3_normalised_contract_fields(raw: dict) -> tuple[str, str, float] | None:
    """Return strict common-base contract evidence from an untrusted quote."""
    if raw.get("contract_normalization_complete") is not True:
        return None
    underlying = str(raw.get("underlying", "") or "").strip().upper()
    quote_currency = str(raw.get("quote_currency", "") or "").strip().upper()
    if (
        not underlying
        or not quote_currency
        or str(raw.get("contract_type", "") or "").lower() != "linear"
        or not str(raw.get("mark_index_source", "") or "").strip()
        or str(raw.get("venue_status", "") or "").lower() != "active"
    ):
        return None
    try:
        multiplier = float(raw.get("contract_multiplier", 0.0) or 0.0)
    except (TypeError, ValueError, OverflowError):
        return None
    price_precision = _v3_positive_int(raw.get("price_precision"))
    quantity_precision = _v3_positive_int(raw.get("quantity_precision"))
    funding_interval_ms = _v3_positive_int(raw.get("funding_interval_ms"))
    if (
        not isfinite(multiplier)
        or multiplier <= 0.0
        or price_precision is None
        or quantity_precision is None
        or funding_interval_ms is None
    ):
        return None
    return underlying, quote_currency, multiplier


def _v3_positive_int(value: object) -> int | None:
    """Accept only a finite, literal positive integer from raw JSON."""
    if isinstance(value, bool):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not isfinite(numeric) or numeric <= 0.0 or not numeric.is_integer():
        return None
    return int(numeric)


def publish_snapshot(snapshot: SidecarSnapshot, path: str | Path) -> None:
    """Write snapshot atomically: temp file, flush, replace."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), prefix=".snapshot-")
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        data = _snapshot_to_dict(snapshot)
        content = json.dumps(data, ensure_ascii=False, indent=2)
        with open(tmp, "w") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(target)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def load_snapshot(path: str | Path) -> SidecarSnapshot | None:
    """Load and validate a sidecar snapshot. Returns None if missing or malformed."""
    p = Path(path)
    if not p.exists():
        return None
    try:
        with open(p) as f:
            data = json.loads(f.read())
    # A syntactically valid snapshot can still carry an invalid scalar from a
    # partial writer, an older publisher, or manual recovery.  Parsing is the
    # compatibility boundary; no caller in the live loop should have to catch
    # a malformed field before deciding whether entry is safe.
    except (json.JSONDecodeError, OSError, TypeError, ValueError, OverflowError):
        return None
    if not isinstance(data, dict):
        return None
    if _safe_int(data.get("schema_version"), default=0) <= 0:
        return None
    try:
        return _dict_to_snapshot(data)
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


def _snapshot_to_dict(s: SidecarSnapshot) -> dict:
    return {
        "schema_version": s.schema_version,
        "published_at_ms": s.published_at_ms,
        "market_observed_at_ms": s.market_observed_at_ms,
        "funding_lifecycle": [
            {"venue": fl.venue, "observed_at_ms": fl.observed_at_ms, "symbol_count": fl.symbol_count,
             "coverage_usable": fl.coverage_usable, "degraded_reason": fl.degraded_reason}
            for fl in s.funding_lifecycle
        ],
        "market_lifecycle": [
            {"venue": ml.venue, "observed_at_ms": ml.observed_at_ms, "symbol_count": ml.symbol_count,
             "coverage_usable": ml.coverage_usable, "degraded_reason": ml.degraded_reason}
            for ml in s.market_lifecycle
        ],
        "transfer_lifecycle": [
            {"from_venue": tl.from_venue, "to_venue": tl.to_venue, "observed_at_ms": tl.observed_at_ms,
             "coverage_usable": tl.coverage_usable, "degraded_reason": tl.degraded_reason}
            for tl in s.transfer_lifecycle
        ],
        "liquidity_lifecycle": [
            {"venue": ll.venue, "observed_at_ms": ll.observed_at_ms, "symbol_count": ll.symbol_count,
             "coverage_usable": ll.coverage_usable, "degraded_reason": ll.degraded_reason,
             "domain": ll.domain, "source": ll.source,
             "publish_interval_ms": ll.publish_interval_ms,
             "published_at_ms": ll.published_at_ms}
            for ll in s.liquidity_lifecycle
        ],
        "degraded_venues": list(s.degraded_venues),
        "degraded_domains": list(s.degraded_domains),
        "degraded_symbols": {k: list(v) for k, v in s.degraded_symbols.items()},
        "source_mode": s.source_mode,
        "acquisition_mode": s.acquisition_mode,
        "quotes": {
            k: {
                "venue": q.venue,
                "symbol": q.symbol,
                "bid": q.bid,
                "ask": q.ask,
                "observed_at_ms": q.observed_at_ms,
                "source": q.source,
                "bid_size": q.bid_size,
                "ask_size": q.ask_size,
                "bid_depth": [list(level) for level in q.bid_depth],
                "ask_depth": [list(level) for level in q.ask_depth],
                "funding_rate_bps": q.funding_rate_bps,
                "funding_timestamp_ms": q.funding_timestamp_ms,
                "funding_interval_ms": q.funding_interval_ms,
                "predicted_funding_rate_bps": q.predicted_funding_rate_bps,
                "funding_forecast_source": q.funding_forecast_source,
                "funding_forecast_sample_count": q.funding_forecast_sample_count,
                "funding_forecast_uncertainty_bps": q.funding_forecast_uncertainty_bps,
                "funding_forecast_started_at_ms": q.funding_forecast_started_at_ms,
                "funding_forecast_distribution_stable": q.funding_forecast_distribution_stable,
                "funding_forecast_stability_reason": q.funding_forecast_stability_reason,
                "funding_forecast_median_drift_bps": q.funding_forecast_median_drift_bps,
                "funding_forecast_p90_drift_bps": q.funding_forecast_p90_drift_bps,
                "settled_funding_rate_bps": q.settled_funding_rate_bps,
                "mark_price": q.mark_price,
                "index_price": q.index_price,
                "volume_24h_quote": q.volume_24h_quote,
                "open_interest": q.open_interest,
                "open_interest_evidence_status": q.open_interest_evidence_status,
                "open_interest_evidence_reason": q.open_interest_evidence_reason,
                "oi_candidate_count": q.oi_candidate_count,
                "oi_cache_hit_count": q.oi_cache_hit_count,
                "oi_cache_miss_count": q.oi_cache_miss_count,
                "oi_refresh_attempt_count": q.oi_refresh_attempt_count,
                "oi_refresh_cap": q.oi_refresh_cap,
                "oi_deferred_count": q.oi_deferred_count,
                "oi_timeout_count": q.oi_timeout_count,
                "oi_refresh_elapsed_ms": q.oi_refresh_elapsed_ms,
                "underlying": q.underlying,
                "quote_currency": q.quote_currency,
                "contract_type": q.contract_type,
                "contract_multiplier": q.contract_multiplier,
                "mark_index_source": q.mark_index_source,
                "price_precision": q.price_precision,
                "quantity_precision": q.quantity_precision,
                "venue_status": q.venue_status,
                "contract_normalization_complete": q.contract_normalization_complete,
            }
            for k, q in s.quotes.items()
        },
        "candidates": [
            {
                "long_venue": c.long_venue,
                "short_venue": c.short_venue,
                "symbol": c.symbol,
                "funding_diff_bps": c.funding_diff_bps,
                "funding_edge_bps": c.funding_edge_bps,
                "expected_edge_bps": c.expected_edge_bps,
                "worst_case_edge_bps": c.worst_case_edge_bps,
                "ranking_edge_bps": c.ranking_edge_bps,
                "total_funding_edge_bps": c.total_funding_edge_bps,
                "first_stage_funding_edge_bps": c.first_stage_funding_edge_bps,
                "first_stage_expected_edge_bps": c.first_stage_expected_edge_bps,
                "first_stage_worst_case_edge_bps": c.first_stage_worst_case_edge_bps,
                "second_stage_incremental_funding_edge_bps": c.second_stage_incremental_funding_edge_bps,
                "second_stage_worst_case_funding_edge_bps": c.second_stage_worst_case_funding_edge_bps,
                "stagger_gap_ms": c.stagger_gap_ms,
                "transfer_bias_bps": c.transfer_bias_bps,
                "entry_cross_bps": c.entry_cross_bps,
                "fee_bps": c.fee_bps,
                "entry_slippage_bps": c.entry_slippage_bps,
                "long_entry_slippage_bps": c.long_entry_slippage_bps,
                "short_entry_slippage_bps": c.short_entry_slippage_bps,
                "long_exit_slippage_bps": c.long_exit_slippage_bps,
                "short_exit_slippage_bps": c.short_exit_slippage_bps,
                "long_taker_fee_bps": c.long_taker_fee_bps,
                "short_taker_fee_bps": c.short_taker_fee_bps,
                "taker_fee_evidence_complete": c.taker_fee_evidence_complete,
                "opportunity_type": c.opportunity_type,
                "blocked": c.blocked,
                "blocked_reasons": c.blocked_reasons,
                "pair_id": c.pair_id,
                "funding_timestamp_ms": c.funding_timestamp_ms,
                "first_funding_timestamp_ms": c.first_funding_timestamp_ms,
                "long_funding_timestamp_ms": c.long_funding_timestamp_ms,
                "short_funding_timestamp_ms": c.short_funding_timestamp_ms,
                "second_funding_timestamp_ms": c.second_funding_timestamp_ms,
                "entry_notional_quote": c.entry_notional_quote,
                "first_funding_leg": c.first_funding_leg,
                "entry_maker_leg": c.entry_maker_leg,
                "exit_maker_leg": c.exit_maker_leg,
                "transfer_state_at_entry": c.transfer_state_at_entry,
                "entry_liquidity_source_at_entry": c.entry_liquidity_source_at_entry,
                "long_volume_24h_quote": c.long_volume_24h_quote,
                "short_volume_24h_quote": c.short_volume_24h_quote,
                "long_open_interest_quote_at_entry": c.long_open_interest_quote_at_entry,
                "short_open_interest_quote_at_entry": c.short_open_interest_quote_at_entry,
                "long_entry_vwap": c.long_entry_vwap,
                "short_entry_vwap": c.short_entry_vwap,
                "entry_capacity_constrained": c.entry_capacity_constrained,
                "entry_target_quantity": c.entry_target_quantity,
                "long_max_executable_quantity": c.long_max_executable_quantity,
                "short_max_executable_quantity": c.short_max_executable_quantity,
                "entry_max_executable_quantity": c.entry_max_executable_quantity,
                "entry_depth_shortfall_quantity": c.entry_depth_shortfall_quantity,
                "entry_max_executable_notional_quote": c.entry_max_executable_notional_quote,
                "entry_depth_capped_at_entry": c.entry_depth_capped_at_entry,
                "advisories": c.advisories,
                "direction_consistent": c.direction_consistent,
                "interval_aligned": c.interval_aligned,
                "sizing_liquidity_source": c.sizing_liquidity_source,
                "gross_signal_edge_bps": c.gross_signal_edge_bps,
                "expected_exit_cross_bps": c.expected_exit_cross_bps,
                "entry_fee_bps": c.entry_fee_bps,
                "exit_fee_bps": c.exit_fee_bps,
                "exit_slippage_bps": c.exit_slippage_bps,
                "adverse_selection_bps": c.adverse_selection_bps,
                "capital_buffer_bps": c.capital_buffer_bps,
                "execution_buffer_bps": c.execution_buffer_bps,
                "venue_risk_haircut_bps": c.venue_risk_haircut_bps,
                "transfer_or_inventory_bias_bps": c.transfer_or_inventory_bias_bps,
                "expected_net_edge_bps": c.expected_net_edge_bps,
                "economics_observed_at_ms": c.economics_observed_at_ms,
                "economics_complete": c.economics_complete,
                "economics_incomplete_reason": c.economics_incomplete_reason,
                "calculation_version": c.calculation_version,
                "model_epoch": c.model_epoch,
                "forecast_long_rate_bps": c.forecast_long_rate_bps,
                "forecast_short_rate_bps": c.forecast_short_rate_bps,
                "forecast_worst_funding_edge_bps": c.forecast_worst_funding_edge_bps,
                "forecast_confidence": c.forecast_confidence,
                "forecast_sample_count": c.forecast_sample_count,
                # This is audit evidence for the mandatory shadow period.  Do
                # not let a publish/load round trip turn a mature forecast
                # back into an apparently cold one.
                "forecast_shadow_age_ms": c.forecast_shadow_age_ms,
                "forecast_ready": c.forecast_ready,
                "forecast_distribution_stable": c.forecast_distribution_stable,
                "forecast_stability_reason": c.forecast_stability_reason,
                "forecast_median_drift_bps": c.forecast_median_drift_bps,
                "forecast_p90_drift_bps": c.forecast_p90_drift_bps,
                "forecast_source": c.forecast_source,
            }
            for c in s.candidates
        ],
    }


def _dict_to_snapshot(d: dict) -> SidecarSnapshot:
    schema_version = _safe_int(d.get("schema_version"), default=0)
    # --- V1 compat: convert V1 Rust sidecar format to V2 (see v1_compat.py) ---
    if schema_version == 1:
        from lightfee.sidecar.v1_compat import convert_v1_snapshot_to_v2
        d = convert_v1_snapshot_to_v2(d)
        schema_version = _safe_int(d.get("schema_version"), default=0)
    # --- end V1 compat ---

    from lightfee.sidecar.snapshot import (
        CandidateInput,
        FundingLifecycle,
        LiquidityLifecycle,
        MarketLifecycle,
        TransferLifecycle,
    )

    quotes_raw = d.get("quotes", {})
    def _enrich_candidate(c: dict) -> CandidateInput:
        """Enrich a candidate dict with V1-required identity + prewarm fields.

        V1: every CandidateOpportunity carries a stable pair_id and a usable
        first_funding_timestamp_ms.  When a schema-2 snapshot omits these
        fields, we derive them from the candidate's own symbol/venues and the
        snapshot quotes so the runtime can apply the prewarm gate correctly.
        If we cannot derive a usable timestamp, the candidate is marked
        blocked — it must not appear tradeable with first_funding_timestamp_ms=0.

        Derivation order (V1 parity):
        1. Raw candidate long_funding_timestamp_ms / short_funding_timestamp_ms
        2. Snapshot quotes for long_venue:symbol and short_venue:symbol
        """
        c = dict(c)
        # v1/v2 snapshots predate the complete economics contract. Keep their
        # displayed legacy fields for V1 recovery/diagnostics, but they can
        # never grant live admission even when a hand-edited old snapshot
        # asserts ``economics_complete=true``.
        raw_economics_complete = c.get("economics_complete", False)
        complete_economics = raw_economics_complete is True
        economics_observed_at_ms = _safe_int(
            c.get("economics_observed_at_ms"),
            default=0,
        )
        contract_reason = (
            _v3_economics_contract_reason(c, quotes_raw=quotes_raw)
            if schema_version >= 3 and complete_economics
            else ""
        )
        if (
            schema_version >= 3
            and raw_economics_complete is not True
            and raw_economics_complete is not False
        ):
            contract_reason = "invalid_v3_economics_field:economics_complete"
        if (
            schema_version < 3
            or (
                schema_version >= 3
                and (
                    not complete_economics
                    or economics_observed_at_ms <= 0
                    or bool(contract_reason)
                )
            )
        ):
            c["economics_complete"] = False
            c.setdefault("calculation_version", "legacy_schema_incomplete")
            c.setdefault("model_epoch", "v1_legacy")
            if contract_reason:
                c["economics_incomplete_reason"] = contract_reason

        pair_id = str(c.get("pair_id", "") or "")
        symbol = str(c.get("symbol", ""))
        long_ven = str(c.get("long_venue", ""))
        short_ven = str(c.get("short_venue", ""))

        if not pair_id and symbol and long_ven and short_ven:
            pair_id = f"{symbol.lower()}:{long_ven}->{short_ven}"

        ff_ts = _safe_int(c.get("first_funding_timestamp_ms"), default=0)
        f_ts = _safe_int(c.get("funding_timestamp_ms"), default=0)
        long_fts = _safe_int(c.get("long_funding_timestamp_ms"), default=0)
        short_fts = _safe_int(c.get("short_funding_timestamp_ms"), default=0)

        # Derive long/short timestamps from quotes if missing in raw candidate
        if long_fts <= 0 or short_fts <= 0:
            for venue, target in [(long_ven, "long"), (short_ven, "short")]:
                qkey = f"{venue}:{symbol}"
                q = quotes_raw.get(qkey, {})
                if isinstance(q, dict):
                    qts = _safe_int(q.get("funding_timestamp_ms"), default=0)
                    if qts > 0:
                        if target == "long" and long_fts <= 0:
                            long_fts = qts
                        elif target == "short" and short_fts <= 0:
                            short_fts = qts

        # Derive first_funding_timestamp_ms from per-leg timestamps (V1: min(long, short))
        if ff_ts <= 0 and long_fts > 0 and short_fts > 0:
            ff_ts = min(long_fts, short_fts)
        elif ff_ts <= 0:
            # Fallback: derive from quotes
            ts_candidates: list[int] = []
            for venue in (long_ven, short_ven):
                qkey = f"{venue}:{symbol}"
                q = quotes_raw.get(qkey, {})
                if isinstance(q, dict):
                    qts = _safe_int(q.get("funding_timestamp_ms"), default=0)
                    if qts > 0:
                        ts_candidates.append(qts)
            if ts_candidates:
                ff_ts = min(ts_candidates)
        if f_ts <= 0 and ff_ts > 0:
            f_ts = ff_ts

        # Compute second_funding_timestamp_ms (V1: max(long, short))
        second_fts = 0
        if long_fts > 0 and short_fts > 0:
            second_fts = max(long_fts, short_fts)

        # Do not let raw JSON scalars bypass the parser boundary.  CandidateInput
        # is a plain dataclass, so an invalid timestamp string would otherwise
        # survive construction and later crash whichever live/readiness path
        # first compares it with an integer.  Rewriting these fields keeps the
        # snapshot readable while the candidate itself remains blocked below.
        c["first_funding_timestamp_ms"] = ff_ts
        c["funding_timestamp_ms"] = f_ts
        c["long_funding_timestamp_ms"] = long_fts
        c["short_funding_timestamp_ms"] = short_fts
        c["second_funding_timestamp_ms"] = second_fts

        candidate = CandidateInput(**c)
        if pair_id:
            candidate.pair_id = pair_id
        if ff_ts > 0:
            candidate.first_funding_timestamp_ms = ff_ts
        if f_ts > 0:
            candidate.funding_timestamp_ms = f_ts
        if long_fts > 0:
            candidate.long_funding_timestamp_ms = long_fts
        if short_fts > 0:
            candidate.short_funding_timestamp_ms = short_fts
        if second_fts > 0:
            candidate.second_funding_timestamp_ms = second_fts

        # V1: first_funding_leg — which leg's funding settles first
        # discovery.rs:850-863 — Long if long_ts <= short_ts, else Short
        if long_fts > 0 and short_fts > 0:
            candidate.first_funding_leg = (
                "long" if long_fts <= short_fts else "short"
            )

        # Fail-closed: candidates without usable funding timestamp are not tradeable.
        # V1 never emits a tradeable candidate with first_funding_timestamp_ms=0.
        if candidate.first_funding_timestamp_ms <= 0:
            candidate.blocked = True
            candidate.blocked_reasons = list(candidate.blocked_reasons) + [
                "missing_candidate_identity_or_funding_timestamp"
            ]

        return candidate

    return SidecarSnapshot(
        schema_version=schema_version,
        published_at_ms=_safe_int(d.get("published_at_ms"), default=0),
        market_observed_at_ms=_safe_int(d.get("market_observed_at_ms"), default=0),
        funding_lifecycle=[FundingLifecycle(**fl) for fl in d.get("funding_lifecycle", [])],
        market_lifecycle=[MarketLifecycle(**ml) for ml in d.get("market_lifecycle", [])],
        transfer_lifecycle=[TransferLifecycle(**tl) for tl in d.get("transfer_lifecycle", [])],
        liquidity_lifecycle=[LiquidityLifecycle(**ll) for ll in d.get("liquidity_lifecycle", [])],
        degraded_venues=d.get("degraded_venues", []),
        degraded_domains=d.get("degraded_domains", []),
        degraded_symbols=_parse_degraded_symbols(d.get("degraded_symbols", {})),
        source_mode=d.get("source_mode", ""),
        acquisition_mode=d.get("acquisition_mode", ""),
        quotes={k: _quote_from_dict(v) for k, v in quotes_raw.items()},
        candidates=[_enrich_candidate(c) for c in d.get("candidates", [])],
    )


def _quote_from_dict(raw: object) -> QuoteSnapshot:
    """Parse optional executable depth at the snapshot compatibility boundary.

    A malformed ladder must never make a historical BBO snapshot unreadable or
    turn arbitrary JSON into executable liquidity.  Drop the malformed ladder
    and retain the independently validated BBO fields; the paper engine will
    then take its conservative top-book-only path.
    """
    if not isinstance(raw, dict):
        raise TypeError("quote must be an object")
    value = dict(raw)
    value["bid_depth"] = _normalise_depth(value.get("bid_depth"))
    value["ask_depth"] = _normalise_depth(value.get("ask_depth"))
    for field in (
        "bid",
        "ask",
        "bid_size",
        "ask_size",
        "funding_rate_bps",
        "funding_forecast_uncertainty_bps",
        "funding_forecast_median_drift_bps",
        "funding_forecast_p90_drift_bps",
        "mark_price",
        "index_price",
        "volume_24h_quote",
        "open_interest",
        "contract_multiplier",
    ):
        if field in value:
            value[field] = _safe_float(value[field], default=0.0)
    for field in ("predicted_funding_rate_bps", "settled_funding_rate_bps"):
        if field in value:
            value[field] = _safe_optional_float(value[field])
    for field in (
        "observed_at_ms",
        "funding_timestamp_ms",
        "funding_interval_ms",
        "funding_forecast_sample_count",
        "funding_forecast_started_at_ms",
        "oi_candidate_count",
        "oi_cache_hit_count",
        "oi_cache_miss_count",
        "oi_refresh_attempt_count",
        "oi_refresh_cap",
        "oi_deferred_count",
        "oi_timeout_count",
        "oi_refresh_elapsed_ms",
        "price_precision",
        "quantity_precision",
    ):
        if field in value:
            value[field] = _safe_int(value[field], default=0)
    # Snapshot JSON is an untrusted compatibility boundary.  Python treats a
    # non-empty string such as ``"false"`` as truthy, which would otherwise
    # let malformed v3 evidence assert forecast stability or contract
    # normalisation.  Only an actual JSON boolean may grant either permission;
    # absent legacy fields retain their dataclass fail-closed defaults.
    for field in (
        "funding_forecast_distribution_stable",
        "contract_normalization_complete",
    ):
        if field in value and value[field] is not True and value[field] is not False:
            value[field] = False
    return QuoteSnapshot(**value)


def _safe_int(value: object, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    if not isfinite(numeric) or numeric <= 0.0 or not numeric.is_integer():
        return default
    return int(numeric)


def _safe_float(value: object, *, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    if not isfinite(numeric):
        return default
    return numeric


def _safe_optional_float(value: object) -> float | None:
    if value is None:
        return None
    parsed = _safe_float(value, default=float("nan"))
    return parsed if isfinite(parsed) else None


def _normalise_depth(raw: object) -> tuple[tuple[float, float], ...]:
    if raw in (None, ""):
        return ()
    if not isinstance(raw, (list, tuple)):
        return ()
    levels: list[tuple[float, float]] = []
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            return ()
        if isinstance(item[0], bool) or isinstance(item[1], bool):
            return ()
        try:
            price = float(item[0])
            quantity = float(item[1])
        except (TypeError, ValueError, OverflowError):
            return ()
        if not (isfinite(price) and isfinite(quantity) and price > 0.0 and quantity > 0.0):
            return ()
        levels.append((price, quantity))
    return tuple(levels)


def _parse_degraded_symbols(raw: object) -> dict[str, list[str]]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, list[str]] = {}
    for k, v in raw.items():
        if isinstance(v, list):
            result[str(k)] = [str(x) for x in v]
    return result
