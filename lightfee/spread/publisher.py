"""Atomic publisher for spread-reversion snapshots."""

from __future__ import annotations

import json
from math import isfinite
import os
import tempfile
from pathlib import Path

from lightfee.spread.models import SpreadReversionCandidate, SpreadSnapshot


_SPREAD_FLOAT_CANDIDATE_FIELDS = {
    "spread_mid_bps",
    "executable_spread_bps",
    "rolling_mean_bps",
    "rolling_std_bps",
    "z_score",
    "net_edge_bps",
    "entry_notional_quote",
    "capacity_quote",
    "fee_bps",
    "slippage_reserve_bps",
    "adverse_selection_buffer_bps",
    "funding_carry_cost_bps",
    "fair_price",
    "venue_premium_bps",
    "fair_price_confidence",
    "mean_reversion_quality",
    "gross_edge_bps",
    "funding_carry_bps",
    "liquidity_score",
    "venue_health_score",
    "score",
    "current_signed_mid_spread_bps",
    "current_executable_entry_spread_bps",
    "equilibrium_spread_bps",
    "target_exit_spread_bps",
    "gross_reversion_edge_bps",
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
    "ranking_edge_bps",
    "expected_net_edge_bps",
    "worst_case_edge_bps",
    "net_edge_per_capital_hour_bps",
    "risk_adjusted_edge_per_capital_hour_bps",
    "hold_time_confidence",
    "dynamic_min_gross_edge_bps",
}

_SPREAD_INT_CANDIDATE_FIELDS = {
    "sample_count",
    "signal_ts_ms",
    "long_quote_ts_ms",
    "short_quote_ts_ms",
    "quote_skew_ms",
    "funding_timestamp_ms",
    "first_funding_timestamp_ms",
    "half_life_ms",
    "hold_time_hint_ms",
    "history_age_ms",
    "economics_observed_at_ms",
    "account_fee_evidence_observed_at_ms",
}


def publish_spread_snapshot(snapshot: SpreadSnapshot, path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), prefix=".spread-snapshot-")
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        content = json.dumps(_snapshot_to_dict(snapshot), ensure_ascii=False, indent=2)
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(target)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def load_spread_snapshot(path: str | Path) -> SpreadSnapshot | None:
    p = Path(path)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(data, dict):
        return None
    schema_version = int(data.get("schema_version", 0) or 0)
    if schema_version not in {1, 2, 3}:
        return None
    candidates = []
    for raw in data.get("candidates", []) or []:
        if isinstance(raw, dict):
            candidate_data = dict(raw)
            if "fair_price" not in candidate_data and "fair_price_bps" in candidate_data:
                candidate_data["fair_price"] = candidate_data.pop("fair_price_bps")
            else:
                candidate_data.pop("fair_price_bps", None)
            # Snapshots are an external JSON boundary.  Python treats a
            # non-empty string such as ``"false"`` as truthy, which must not
            # promote a malformed payload into a paper or live-ready candidate.
            # Missing fields retain the model defaults (False); only literal
            # JSON ``true`` is admissible as positive evidence.
            for field in (
                "economics_complete",
                "fee_evidence_complete",
                "account_fee_evidence_complete",
            ):
                if field in candidate_data and candidate_data[field] is not True:
                    candidate_data[field] = False
            provenance = candidate_data.get("account_fee_evidence_provenance")
            if not isinstance(provenance, list) or any(
                not isinstance(row, dict) for row in provenance
            ):
                candidate_data["account_fee_evidence_provenance"] = []
            # Schema v1 predates signed-basis economics.  It remains readable
            # for diagnostics, but is explicitly separated from the v2 paper
            # cohort and cannot be mistaken for complete reversion economics.
            if schema_version == 1:
                candidate_data["model_epoch"] = "v1_legacy"
                candidate_data["calculation_version"] = "spread_v1_legacy"
                candidate_data["economics_complete"] = False
            _sanitize_candidate_numbers(candidate_data)
            candidates.append(SpreadReversionCandidate(**candidate_data))
    return SpreadSnapshot(
        schema_version=schema_version,
        published_at_ms=_safe_int(data.get("published_at_ms", 0)),
        market_observed_at_ms=_safe_int(data.get("market_observed_at_ms", 0)),
        snapshot_path=str(data.get("snapshot_path", "") or ""),
        source_mode=str(data.get("source_mode", "") or ""),
        degraded_venues=list(data.get("degraded_venues", []) or []),
        degraded_symbols=dict(data.get("degraded_symbols", {}) or {}),
        rejection_counts={
            str(key): _safe_int(value)
            for key, value in dict(data.get("rejection_counts", {}) or {}).items()
        },
        candidates=candidates,
    )


def _snapshot_to_dict(snapshot: SpreadSnapshot) -> dict:
    return {
        "schema_version": snapshot.schema_version,
        "published_at_ms": snapshot.published_at_ms,
        "market_observed_at_ms": snapshot.market_observed_at_ms,
        "snapshot_path": snapshot.snapshot_path,
        "source_mode": snapshot.source_mode,
        "degraded_venues": list(snapshot.degraded_venues),
        "degraded_symbols": {
            str(key): list(value)
            for key, value in snapshot.degraded_symbols.items()
        },
        "rejection_counts": {
            str(key): int(value)
            for key, value in snapshot.rejection_counts.items()
        },
        "candidates": [
            {
                "candidate_id": c.candidate_id,
                "symbol": c.symbol,
                "long_venue": c.long_venue,
                "short_venue": c.short_venue,
                "spread_mid_bps": c.spread_mid_bps,
                "executable_spread_bps": c.executable_spread_bps,
                "rolling_mean_bps": c.rolling_mean_bps,
                "rolling_std_bps": c.rolling_std_bps,
                "z_score": c.z_score,
                "net_edge_bps": c.net_edge_bps,
                "sample_count": c.sample_count,
                "signal_ts_ms": c.signal_ts_ms,
                "long_quote_ts_ms": c.long_quote_ts_ms,
                "short_quote_ts_ms": c.short_quote_ts_ms,
                "entry_notional_quote": c.entry_notional_quote,
                "capacity_quote": c.capacity_quote,
                "signal_status": c.signal_status,
                "strategy_bucket": c.strategy_bucket,
                "fee_bps": c.fee_bps,
                "slippage_reserve_bps": c.slippage_reserve_bps,
                "adverse_selection_buffer_bps": c.adverse_selection_buffer_bps,
                "funding_carry_cost_bps": c.funding_carry_cost_bps,
                "quote_skew_ms": c.quote_skew_ms,
                "funding_timestamp_ms": c.funding_timestamp_ms,
                "first_funding_timestamp_ms": c.first_funding_timestamp_ms,
                "fair_price": c.fair_price,
                "venue_premium_bps": c.venue_premium_bps,
                "fair_price_confidence": c.fair_price_confidence,
                "mean_reversion_quality": c.mean_reversion_quality,
                "half_life_ms": c.half_life_ms,
                "hold_time_hint_ms": c.hold_time_hint_ms,
                "gross_edge_bps": c.gross_edge_bps,
                "funding_carry_bps": c.funding_carry_bps,
                "liquidity_score": c.liquidity_score,
                "venue_health_score": c.venue_health_score,
                "score": c.score,
                "rank_reason": c.rank_reason,
                "degradation_state": c.degradation_state,
                "liquidity_evidence_status": c.liquidity_evidence_status,
                "screening_reasons": list(c.screening_reasons),
                "history_age_ms": c.history_age_ms,
                "opportunity_label": c.opportunity_label,
                "canonical_venue_a": c.canonical_venue_a,
                "canonical_venue_b": c.canonical_venue_b,
                "current_signed_mid_spread_bps": c.current_signed_mid_spread_bps,
                "current_executable_entry_spread_bps": c.current_executable_entry_spread_bps,
                "equilibrium_spread_bps": c.equilibrium_spread_bps,
                "target_exit_spread_bps": c.target_exit_spread_bps,
                "gross_reversion_edge_bps": c.gross_reversion_edge_bps,
                "gross_signal_edge_bps": c.gross_signal_edge_bps,
                "funding_edge_bps": c.funding_edge_bps,
                "entry_cross_bps": c.entry_cross_bps,
                "expected_exit_cross_bps": c.expected_exit_cross_bps,
                "entry_fee_bps": c.entry_fee_bps,
                "exit_fee_bps": c.exit_fee_bps,
                "entry_slippage_bps": c.entry_slippage_bps,
                "exit_slippage_bps": c.exit_slippage_bps,
                "adverse_selection_bps": c.adverse_selection_bps,
                "capital_buffer_bps": c.capital_buffer_bps,
                "execution_buffer_bps": c.execution_buffer_bps,
                "venue_risk_haircut_bps": c.venue_risk_haircut_bps,
                "transfer_or_inventory_bias_bps": c.transfer_or_inventory_bias_bps,
                "ranking_edge_bps": c.ranking_edge_bps,
                "economics_observed_at_ms": c.economics_observed_at_ms,
                "expected_net_edge_bps": c.expected_net_edge_bps,
                "worst_case_edge_bps": c.worst_case_edge_bps,
                "calculation_version": c.calculation_version,
                "model_epoch": c.model_epoch,
                "economics_complete": c.economics_complete,
                "fee_evidence_complete": c.fee_evidence_complete,
                "account_fee_evidence_complete": c.account_fee_evidence_complete,
                "account_fee_evidence_observed_at_ms": (
                    c.account_fee_evidence_observed_at_ms
                ),
                "account_fee_evidence_source": c.account_fee_evidence_source,
                "account_fee_evidence_fingerprint": c.account_fee_evidence_fingerprint,
                "account_fee_evidence_provenance": list(
                    c.account_fee_evidence_provenance
                ),
                "research_sample_split": c.research_sample_split,
                "volatility_regime": c.volatility_regime,
                "net_edge_per_capital_hour_bps": c.net_edge_per_capital_hour_bps,
                "risk_adjusted_edge_per_capital_hour_bps": (
                    c.risk_adjusted_edge_per_capital_hour_bps
                ),
                "hold_time_confidence": c.hold_time_confidence,
                "dynamic_min_gross_edge_bps": c.dynamic_min_gross_edge_bps,
                "contract_normalization_status": c.contract_normalization_status,
                "contract_normalization_reason": c.contract_normalization_reason,
            }
            for c in snapshot.candidates
        ],
    }


def _sanitize_candidate_numbers(candidate_data: dict) -> None:
    invalid_fields: list[str] = []
    for field in _SPREAD_FLOAT_CANDIDATE_FIELDS:
        if field not in candidate_data:
            continue
        value = _safe_float(candidate_data[field])
        if value is None:
            invalid_fields.append(field)
            candidate_data[field] = 0.0
        else:
            candidate_data[field] = value
    for field in _SPREAD_INT_CANDIDATE_FIELDS:
        if field not in candidate_data:
            continue
        value = _safe_int_or_none(candidate_data[field])
        if value is None:
            invalid_fields.append(field)
            candidate_data[field] = 0
        else:
            candidate_data[field] = value
    if not invalid_fields:
        return
    candidate_data["economics_complete"] = False
    reasons = candidate_data.get("screening_reasons", [])
    if not isinstance(reasons, list):
        reasons = []
    for field in sorted(invalid_fields):
        reasons.append(f"spread_snapshot_invalid_numeric:{field}")
    candidate_data["screening_reasons"] = reasons


def _safe_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return numeric if isfinite(numeric) else None


def _safe_int(value: object) -> int:
    return _safe_int_or_none(value) or 0


def _safe_int_or_none(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not isfinite(numeric):
        return None
    return max(int(numeric), 0)
