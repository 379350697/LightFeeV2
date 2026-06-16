#!/usr/bin/env python3
"""Summarize production entry/local-L2 blockers from a JSONL journal.

The script is intentionally standalone: it reads only the supplied JSONL file
and emits a JSON report that can run on a cloud host in dry-run/read-only mode.

Usage:
  python3 scripts/analyze_production_blockers.py \
    --events /opt/lightfee-v2/runtime/live-events.jsonl \
    --state /opt/lightfee-v2/runtime/live-state.json \
    --snapshot /opt/lightfee-v2/runtime/opportunity-input-snapshot.json \
    --windows last_2h,last_24h,run_window \
    --no-secrets
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from lightfee.marketdata.local_l2_incident_classification import (
    has_official_sequence_rebuild_evidence,
)


def _parse_since_ms(value: str | None) -> int:
    if not value:
        return 0
    dt = datetime.fromisoformat(value)
    return int(dt.timestamp() * 1000)


def _iter_records(path: Path):
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(record, dict):
                yield record


def _payload(record: dict[str, Any]) -> dict[str, Any]:
    payload = record.get("payload", {})
    return payload if isinstance(payload, dict) else {}


def _inc_symbol_pair(
    payload: dict[str, Any],
    top_pairs: Counter[str],
    top_symbols: Counter[str],
) -> None:
    pair_id = str(payload.get("pair_id", "") or "")
    symbol = str(payload.get("symbol", "") or "")
    if pair_id:
        top_pairs[pair_id] += 1
    if symbol:
        top_symbols[symbol] += 1


def _is_entry_admission_selection(reason: str, payload: dict[str, Any]) -> bool:
    source = str(payload.get("source") or "")
    domain = str(payload.get("domain") or "")
    blocker_family = str(payload.get("blocker_family") or "")
    return (
        reason.endswith("_admission_blocked")
        or reason == "bybit_trading_terms_required"
        or source == "entry_admission"
        or domain == "entry_admission"
        or blocker_family == "exchange_admission"
    )


def _snapshot_fallback_blocked(payload: dict[str, Any]) -> bool:
    if _snapshot_fallback_blocking_scope(payload):
        return True
    if payload.get("blocked") is True or payload.get("block_reason"):
        return True
    return False


def _snapshot_fallback_blocking_scope(payload: dict[str, Any]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for item in payload.get("candidate_freshness_scope", []) or []:
        if isinstance(item, dict) and (
            item.get("blocked") is True or item.get("block_reason")
        ):
            blockers.append(item)
    return blockers


def _snapshot_fallback_has_scoped_blocking_evidence(payload: dict[str, Any]) -> bool:
    for item in _snapshot_fallback_blocking_scope(payload):
        if not (item.get("candidate_symbol") or item.get("candidate_pair_id")):
            continue
        if (
            item.get("domain")
            or item.get("venue")
            or item.get("source_age_ms") is not None
            or item.get("fallback_duration_ms") is not None
        ):
            return True
    return False


def _snapshot_fallback_conclusion(payload: dict[str, Any]) -> str:
    if payload.get("v1_parity_evidence"):
        return "v1_parity"
    if _snapshot_fallback_has_scoped_blocking_evidence(payload):
        return "v1_parity"
    return "insufficient_evidence"


def _add_count(counter: Counter[str], key: str, count: Any = 1) -> None:
    try:
        value = int(count or 0)
    except (TypeError, ValueError):
        value = 0
    if value > 0:
        counter[key] += value


def _add_mapping_total(counter: Counter[str], key: str, values: Any) -> None:
    if isinstance(values, dict):
        for count in values.values():
            _add_count(counter, key, count)
    else:
        _add_count(counter, key, values)


def _mapping_has_counts(values: Any) -> bool:
    if not isinstance(values, dict):
        return False
    for count in values.values():
        try:
            if int(count or 0) > 0:
                return True
        except (TypeError, ValueError):
            continue
    return False


_STRATEGY_BLOCKER_REASONS = {
    "funding_edge_below_floor",
    "expected_edge_below_floor",
    "worst_case_edge_below_floor",
    "zero_order_size",
    "funding_window_passed",
    "outside_scan_window",
    "no_near_term_settlement",
    "stagger_gap_too_wide",
    "missing_candidate_identity_or_funding_timestamp",
}


def _is_strategy_blocker_reason(reason: str) -> bool:
    return reason in _STRATEGY_BLOCKER_REASONS


def _is_open_interest_blocker_reason(reason: str) -> bool:
    return (
        "open_interest" in reason
        or reason.startswith("oi_")
        or reason.startswith("perp_oi_")
    )


def _is_liquidity_blocker_reason(reason: str) -> bool:
    return "liquidity" in reason or reason.startswith("execution_")


def _add_reason_total(
    counter: Counter[str],
    key: str,
    values: Any,
    predicate,
) -> None:
    if not isinstance(values, dict):
        return
    for reason, count in values.items():
        if predicate(str(reason)):
            _add_count(counter, key, count)


def _add_total_or_mapping(
    counter: Counter[str],
    key: str,
    total: Any,
    values: Any,
) -> bool:
    if total is not None:
        _add_count(counter, key, total)
        return True
    _add_mapping_total(counter, key, values)
    return _mapping_has_counts(values)


def _is_nonblocking_bulk_probe(payload: dict[str, Any]) -> bool:
    truth_required_by = payload.get("truth_required_by") or []
    return not truth_required_by and payload.get("blocking") is not True


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_hyperliquid_unified_collateral_sample(sample: Any) -> bool:
    if not isinstance(sample, dict):
        return False
    venue = str(sample.get("venue", "") or "").lower()
    classification = str(
        sample.get("balance_classification")
        or sample.get("classification")
        or sample.get("collateral_classification")
        or ""
    )
    user_abstraction = str(sample.get("user_abstraction", "") or "")
    spot_available = _float_or_none(
        sample.get("spot_usdc_available")
        if sample.get("spot_usdc_available") is not None
        else sample.get("usdc_available")
    )
    return (
        venue == "hyperliquid"
        and classification == "unified_collateral_available"
        and user_abstraction == "unifiedAccount"
        and spot_available is not None
        and spot_available > 1e-9
    )


def _unified_collateral_admission_counts(payload: dict[str, Any]) -> Counter[str]:
    ignored: Counter[str] = Counter()
    for sample_key in (
        "entry_admission_venue_degraded_samples",
        "entry_admission_blocker_samples",
    ):
        for sample in payload.get(sample_key, []) or []:
            if not _is_hyperliquid_unified_collateral_sample(sample):
                continue
            reason = str(
                sample.get("reason") or "insufficient_margin_admission_prefiltered"
            )
            ignored[reason] += 1
    return ignored


def _code_side_view(
    *,
    category_counts: Counter[str],
    reason_counts: Counter[str],
    resolution_counts: Counter[str] | None = None,
    oi_evidence_health_summary: Counter[str] | None = None,
    filtered_out_counts: Counter[str],
    exclude_strategy: bool,
    exclude_liquidity: bool,
    enabled: bool | None = None,
) -> dict[str, Any]:
    view_enabled = bool(exclude_strategy or exclude_liquidity) if enabled is None else enabled
    if not view_enabled:
        return {
            "enabled": False,
            "excluded_filters": [],
            "category_counts": {},
            "reason_counts": {},
            "resolution_counts": {},
            "oi_evidence_health_summary": {},
            "filtered_out_counts": {},
        }
    resolution_counts = resolution_counts or Counter()
    oi_evidence_health_summary = oi_evidence_health_summary or Counter()
    return {
        "enabled": True,
        "excluded_filters": [
            name
            for name, enabled in (
                ("strategy", exclude_strategy),
                ("liquidity", exclude_liquidity),
                ("open_interest", exclude_liquidity),
            )
            if enabled
        ],
        "category_counts": dict(sorted(category_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "resolution_counts": dict(sorted(resolution_counts.items())),
        "oi_evidence_health_summary": dict(
            sorted(oi_evidence_health_summary.items())
        ),
        "filtered_out_counts": dict(sorted(filtered_out_counts.items())),
    }


def _record_code_side_blocker(
    *,
    kind: str,
    payload: dict[str, Any],
    category_counts: Counter[str],
    reason_counts: Counter[str],
    resolution_counts: Counter[str],
    oi_evidence_health_summary: Counter[str],
    filtered_out_counts: Counter[str],
    exclude_strategy: bool,
    exclude_liquidity: bool,
) -> None:
    if kind == "scan.no_entry_diagnostics":
        for key in (
            "quote_revalidate_resolved_count",
            "quote_revalidate_failed_count",
            "quote_truth_must_resolve_count",
            "quote_truth_resolved_count",
            "quote_truth_failed_count",
            "quote_truth_ws_resolved_count",
            "quote_truth_rest_resolved_count",
        ):
            try:
                count = int(payload.get(key, 0) or 0)
            except (TypeError, ValueError):
                count = 0
            if count > 0:
                _add_count(
                    resolution_counts,
                    key.replace("_count", ""),
                    count,
                )
        try:
            budget_without_rest = int(
                payload.get("budget_excluded_without_rest_count", 0) or 0
            )
        except (TypeError, ValueError):
            budget_without_rest = 0
        if budget_without_rest > 0:
            _add_count(category_counts, "ws_bbo_budget", budget_without_rest)
            _add_count(reason_counts, "budget_excluded_without_rest", budget_without_rest)
        top_quote_blockers = payload.get("top_quote_blocker_buckets", {}) or {}
        if isinstance(top_quote_blockers, dict):
            for reason, count in top_quote_blockers.items():
                reason_text = str(reason or "quote_revalidate_failed")
                _add_count(category_counts, "code_data_freshness", count)
                _add_count(reason_counts, reason_text, count)
        ws_bbo_totals = payload.get("entry_ws_bbo_blocker_counts", {}) or {}
        for reason, count in ws_bbo_totals.items():
            reason_text = str(reason)
            if reason_text == "entry_ws_bbo_quote_lease_budget_exhausted":
                _add_count(category_counts, "ws_bbo_budget", count)
                _add_count(reason_counts, reason_text, count)
        snapshot_totals = payload.get("snapshot_freshness_blocked_counts", {}) or {}
        for reason, count in snapshot_totals.items():
            reason_text = str(reason)
            if reason_text in {"invalid_quote", "quote_stale", "missing_quote"}:
                _add_count(category_counts, "code_data_freshness", count)
                _add_count(reason_counts, reason_text, count)
        unified_collateral_ignored = _unified_collateral_admission_counts(payload)
        for admission_key in (
            "entry_admission_blocker_counts",
            "entry_admission_venue_degraded_counts",
        ):
            admission_totals = payload.get(admission_key, {}) or {}
            if not isinstance(admission_totals, dict):
                continue
            for reason, count in admission_totals.items():
                reason_text = str(reason or "entry_admission_blocked")
                try:
                    effective_count = int(count or 0)
                except (TypeError, ValueError):
                    effective_count = 0
                ignored_count = min(
                    effective_count,
                    unified_collateral_ignored.get(reason_text, 0),
                )
                if ignored_count > 0:
                    unified_collateral_ignored[reason_text] -= ignored_count
                    effective_count -= ignored_count
                _add_count(category_counts, "account/admission", effective_count)
                _add_count(reason_counts, reason_text, effective_count)
        if exclude_strategy:
            strategy_counts = payload.get("strategy_blocker_counts", {}) or {}
            strategy_total = payload.get("strategy_blocked_count")
            strategy_counted = _add_total_or_mapping(
                filtered_out_counts,
                "strategy",
                strategy_total,
                strategy_counts,
            )
            if not strategy_counted:
                _add_reason_total(
                    filtered_out_counts,
                    "strategy",
                    payload.get("blocked_reason_counts", {}) or {},
                    _is_strategy_blocker_reason,
                )
        if exclude_liquidity:
            liquidity_counts = payload.get("liquidity_blocker_counts", {}) or {}
            open_interest_counts = payload.get("open_interest_blocker_counts", {}) or {}
            execution_liquidity_counts = (
                payload.get("execution_liquidity_blocked_counts", {}) or {}
            )
            liquidity_total = payload.get("liquidity_blocked_count")
            if liquidity_total is not None:
                _add_count(filtered_out_counts, "liquidity", liquidity_total)
                liquidity_counted = True
            else:
                _add_mapping_total(
                    filtered_out_counts,
                    "liquidity",
                    liquidity_counts,
                )
                _add_mapping_total(
                    filtered_out_counts,
                    "liquidity",
                    execution_liquidity_counts,
                )
                liquidity_counted = (
                    _mapping_has_counts(liquidity_counts)
                    or _mapping_has_counts(execution_liquidity_counts)
                )
            oi_counted = _add_total_or_mapping(
                filtered_out_counts,
                "open_interest",
                payload.get("open_interest_blocked_count"),
                open_interest_counts,
            )
            blocked_reason_counts = payload.get("blocked_reason_counts", {}) or {}
            if not liquidity_counted:
                _add_reason_total(
                    filtered_out_counts,
                    "liquidity",
                    blocked_reason_counts,
                    _is_liquidity_blocker_reason,
                )
            if not oi_counted:
                _add_reason_total(
                    filtered_out_counts,
                    "open_interest",
                    blocked_reason_counts,
                    _is_open_interest_blocker_reason,
                )

    if kind == "runtime.snapshot_freshness_decision":
        reason = str(payload.get("reason", "") or "")
        if reason in {"invalid_quote", "quote_stale", "missing_quote"}:
            _add_count(category_counts, "code_data_freshness")
            _add_count(reason_counts, reason)

    if kind == "runtime.entry_blocked_ws_bbo_selection":
        reason = str(payload.get("reason", "") or "")
        if reason == "entry_ws_bbo_quote_lease_budget_exhausted":
            _add_count(category_counts, "ws_bbo_budget")
            _add_count(reason_counts, reason)

    if kind == "runtime.entry_quote_revalidate_resolved":
        _add_count(resolution_counts, "quote_revalidate_resolved")
        source = str(payload.get("source", "") or "")
        if source:
            _add_count(resolution_counts, f"quote_revalidate_source:{source}")

    if kind == "runtime.last_good_revalidated_by_entry_quote_truth":
        _add_count(resolution_counts, "last_good_revalidated")

    if kind == "runtime.entry_quote_revalidate_failed":
        _add_count(category_counts, "code_data_freshness")
        outcome = str(
            payload.get("reason_bucket")
            or payload.get("outcome")
            or payload.get("reason")
            or "quote_revalidate_failed"
        )
        _add_count(reason_counts, outcome)
        family = str(payload.get("reason_family") or "")
        if family:
            _add_count(reason_counts, f"quote_family:{family}")
        _add_count(resolution_counts, "quote_revalidate_failed")

    if kind == "runtime.live_scan_revalidate_required":
        fallback_source = str(payload.get("fallback_source", "") or "")
        reason = str(payload.get("reason", "") or "")
        if (
            payload.get("targeted_revalidate_required") is True
            or fallback_source == "last_good_sidecar"
            or "last_good_sidecar" in reason
        ):
            _add_count(category_counts, "code_data_freshness")
            _add_count(reason_counts, "last_good_sidecar_revalidate_required")

    if kind == "runtime.snapshot_fallback_last_good" and _snapshot_fallback_blocked(payload):
        blocked_scope_count = len(_snapshot_fallback_blocking_scope(payload)) or 1
        _add_count(category_counts, "code_data_freshness", blocked_scope_count)
        _add_count(
            reason_counts,
            "last_good_sidecar_revalidate_required",
            blocked_scope_count,
        )

    if (
        kind == "recovery.live_position_bulk_diagnostic_error"
        and _is_nonblocking_bulk_probe(payload)
    ):
        _add_count(category_counts, "exchange_truth_probe")
        classification = str(payload.get("classification", "") or "")
        reason = (
            "bulk_position_probe_timeout"
            if classification == "timeout"
            else "bulk_position_probe_diagnostic_error"
        )
        _add_count(reason_counts, reason)

    if kind in {
        "exit.passive_close_hedge_ack_pending_reconcile",
        "exit.accepted_order_truth_gap_registered",
    } or payload.get("accepted_order_truth_gap") is True:
        _add_count(category_counts, "order_truth_gap")
        _add_count(reason_counts, "accepted_order_truth_gap")

    if kind == "execution.entry_liquidity_blocked":
        if exclude_liquidity:
            _add_count(filtered_out_counts, "liquidity")
            return
        _add_count(category_counts, "liquidity")
        reason = str(payload.get("reason") or "entry_liquidity_blocked")
        _add_count(reason_counts, reason)
        oi_status = str(payload.get("open_interest_evidence_status") or "")
        if oi_status:
            _add_count(reason_counts, f"oi_evidence_status:{oi_status}")
        oi_reason = str(payload.get("open_interest_evidence_reason") or "")
        if oi_reason:
            _add_count(reason_counts, f"oi_evidence_reason:{oi_reason}")
        for field in (
            "oi_cache_hit_count",
            "oi_cache_miss_count",
            "oi_refresh_attempt_count",
            "oi_deferred_count",
            "oi_timeout_count",
            "oi_refresh_cap",
            "oi_refresh_elapsed_ms",
        ):
            value = int(payload.get(field) or 0)
            if value:
                _add_count(oi_evidence_health_summary, field, value)

    if kind in {
        "runtime.entry_oi_targeted_refresh_resolved",
        "runtime.entry_oi_targeted_refresh_failed",
    }:
        if exclude_liquidity:
            _add_count(filtered_out_counts, "liquidity")
            return
        _add_count(category_counts, "liquidity_evidence")
        status = str(payload.get("open_interest_evidence_status") or "unknown")
        reason = str(payload.get("open_interest_evidence_reason") or "unknown")
        _add_count(reason_counts, f"oi_targeted_status:{status}")
        _add_count(reason_counts, f"oi_targeted_reason:{reason}")
        if kind == "runtime.entry_oi_targeted_refresh_resolved":
            _add_count(oi_evidence_health_summary, "oi_targeted_resolved_count")
        else:
            _add_count(oi_evidence_health_summary, "oi_targeted_failed_count")
        elapsed_ms = int(payload.get("elapsed_ms") or 0)
        if elapsed_ms:
            oi_evidence_health_summary["oi_targeted_max_elapsed_ms"] = max(
                int(oi_evidence_health_summary.get("oi_targeted_max_elapsed_ms", 0)),
                elapsed_ms,
            )


def build_code_side_blocker_view(
    records: list[dict[str, Any]],
    *,
    exclude_strategy: bool = False,
    exclude_liquidity: bool = False,
    enabled: bool | None = None,
) -> dict[str, Any]:
    category_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    resolution_counts: Counter[str] = Counter()
    oi_evidence_health_summary: Counter[str] = Counter()
    filtered_out_counts: Counter[str] = Counter()
    for record in records:
        payload = _payload(record)
        _record_code_side_blocker(
            kind=str(record.get("kind", "") or ""),
            payload=payload,
            category_counts=category_counts,
            reason_counts=reason_counts,
            resolution_counts=resolution_counts,
            oi_evidence_health_summary=oi_evidence_health_summary,
            filtered_out_counts=filtered_out_counts,
            exclude_strategy=exclude_strategy,
            exclude_liquidity=exclude_liquidity,
        )
    return _code_side_view(
        category_counts=category_counts,
        reason_counts=reason_counts,
        resolution_counts=resolution_counts,
        oi_evidence_health_summary=oi_evidence_health_summary,
        filtered_out_counts=filtered_out_counts,
        exclude_strategy=exclude_strategy,
        exclude_liquidity=exclude_liquidity,
        enabled=enabled,
    )


def _has_official_sequence_evidence(payload: dict[str, Any]) -> bool:
    return has_official_sequence_rebuild_evidence(payload)


def _canonical_okx_symbol(value: Any) -> str:
    raw = str(value or "").upper()
    if raw.endswith("-USDT-SWAP"):
        return raw.replace("-USDT-SWAP", "USDT")
    if raw.endswith("-SWAP"):
        return raw.replace("-SWAP", "")
    return raw.replace("-", "")


def _okx_catalog_symbols(payload: dict[str, Any]) -> set[str]:
    symbols: set[str] = set()
    for row in payload.get("okx_catalog", []) or []:
        if not isinstance(row, dict):
            continue
        inst_id = row.get("instId") or row.get("inst_id") or row.get("symbol")
        if inst_id:
            symbols.add(_canonical_okx_symbol(inst_id))
    return symbols


def _okx_instrument_missing_skipped_count(payload: dict[str, Any]) -> int:
    skipped = payload.get("skipped_by_catalog", []) or []
    if isinstance(skipped, list) and skipped:
        return len(skipped)
    if isinstance(skipped, str) and skipped:
        return 1

    probe_symbols = {
        _canonical_okx_symbol(symbol)
        for symbol in (payload.get("probe_symbols", []) or [])
        if symbol
    }
    catalog_symbols = _okx_catalog_symbols(payload)
    if probe_symbols and catalog_symbols:
        return len(probe_symbols - catalog_symbols)

    return 1 if payload.get("instrument_missing_error") else 0


def _classify_blockers(counts_2h: dict[str, int], counts_24h: dict[str, int],
                       counts_run: dict[str, int]) -> dict[str, str]:
    """Classify each blocker type as current vs historical."""
    classification: dict[str, str] = {}
    high_freq_reasons = {
        "entry_local_l2_waiting_for_primary_tracking",
        "entry_local_l2_waiting_for_dual_ready",
        "entry_local_l2_waiting_for_prewarm_window",
    }
    historical_only_substrings = (
        "entry_waiting_for_finalization_window_too_early",
    )
    exchange_residual_substrings = (
        "min_notional_rejected",
        "order.reconcile_result",
        "uncertain",
    )
    old_recur_reasons = {
        "book_bootstrapping",
        "book_rebuilding",
        "stale_book",
        "crossed_or_locked_book",
        "runtime.local_l2_hot_stale_rebuild",
    }

    def _matches_any(reason: str, substrings: tuple[str, ...]) -> bool:
        return any(s in reason for s in substrings)

    all_reasons = set(counts_run.keys())
    for reason in all_reasons:
        c2h = counts_2h.get(reason, 0)
        c24h = counts_24h.get(reason, 0)
        if c2h == 0 and c24h == 0:
            classification[reason] = "historical_only"
        elif _matches_any(reason, historical_only_substrings):
            classification[reason] = "historical_only"
        elif _matches_any(reason, exchange_residual_substrings):
            classification[reason] = "exchange_rule_residual"
        elif reason in old_recur_reasons:
            classification[reason] = "old_issue_recurred_with_book_reason"
        elif reason in high_freq_reasons and c2h > 0:
            classification[reason] = "current_new_high_frequency"
        else:
            classification[reason] = "current_new_high_frequency" if c2h > 0 else "old_issue_recurred_with_book_reason"
    return classification


def analyze_event_file(
    path: Path | str,
    now_ms: int = 0,
    windows: list[str] | None = None,
    *,
    exclude_strategy: bool = False,
    exclude_liquidity: bool = False,
) -> dict[str, Any]:
    """Analyze a JSONL event file with windowed breakdowns.

    Args:
        path: Path to the JSONL events file.
        now_ms: Reference timestamp for window boundaries (0 = use last event).
        windows: Window names to compute (e.g. ["last_2h", "last_24h", "run_window"]).

    Returns dict with:
        windows: {window_name: {blocker_counts, event_counts, ...}}
        classification: {reason: classification_label}
        first_ts_ms, last_ts_ms
    """
    if windows is None:
        windows = ["last_2h", "last_24h", "run_window"]

    path = Path(path) if isinstance(path, str) else path

    # Collect all records first to compute windows
    all_records: list[dict[str, Any]] = []
    first_ts_ms = 0
    last_ts_ms = 0
    for record in _iter_records(path):
        ts_ms = int(record.get("ts_ms", 0) or 0)
        all_records.append(record)
        if first_ts_ms == 0 or ts_ms < first_ts_ms:
            first_ts_ms = ts_ms
        if ts_ms > last_ts_ms:
            last_ts_ms = ts_ms

    if now_ms <= 0:
        now_ms = last_ts_ms

    # Window boundaries
    h2_ms = 2 * 3600 * 1000
    h24_ms = 24 * 3600 * 1000

    window_bounds: dict[str, int] = {
        "last_2h": now_ms - h2_ms,
        "last_24h": now_ms - h24_ms,
        "run_window": 0,
    }

    def _analyze_records(records: list[dict[str, Any]], since_ms: int) -> dict[str, Any]:
        event_counts: Counter[str] = Counter()
        entry_l2_blocker_counts: Counter[str] = Counter()
        entry_ws_bbo_blocker_counts: Counter[str] = Counter()
        entry_admission_blocker_counts: Counter[str] = Counter()
        top_pairs: Counter[str] = Counter()
        top_symbols: Counter[str] = Counter()
        entry_l2_not_ready_reason_counts: Counter[str] = Counter()
        snapshot_degraded_counts: Counter[str] = Counter()
        snapshot_stale_counts: Counter[str] = Counter()
        order_event_counts: Counter[str] = Counter()
        exchange_error_counts: Counter[str] = Counter()
        pending_entry_counts: Counter[str] = Counter()
        incident_counts: Counter[str] = Counter()
        code_side_category_counts: Counter[str] = Counter()
        code_side_reason_counts: Counter[str] = Counter()
        code_side_resolution_counts: Counter[str] = Counter()
        code_side_oi_evidence_health_summary: Counter[str] = Counter()
        code_side_filtered_counts: Counter[str] = Counter()
        nonblocking_bulk_probe_by_venue: Counter[str] = Counter()
        nonblocking_bulk_probe_details: dict[tuple[str, str], dict[str, Any]] = {}
        candidate_starvation_reasons: Counter[str] = Counter()
        candidate_starvation_symbols: Counter[str] = Counter()
        incident_conclusions: dict[str, str] = {}
        w_first = 0
        w_last = 0

        def _record_candidate_starvation(
            reason: Any,
            count: Any = 1,
            payload: dict[str, Any] | None = None,
        ) -> None:
            reason_key = str(reason or "")
            if not reason_key:
                return
            if (
                "quote_stale" not in reason_key
                and "open_interest" not in reason_key
                and "perp_oi" not in reason_key
                and not reason_key.startswith("oi_")
            ):
                return
            _add_count(candidate_starvation_reasons, reason_key, count)
            if payload:
                symbol = str(payload.get("symbol", "") or "")
                if symbol:
                    _add_count(candidate_starvation_symbols, symbol, count)

        def _record_nonblocking_bulk_probe(payload: dict[str, Any], ts_ms: int) -> None:
            if not _is_nonblocking_bulk_probe(payload):
                return
            venue = str(payload.get("venue") or "unknown").lower()
            endpoint = str(
                payload.get("endpoint")
                or payload.get("path")
                or payload.get("url")
                or payload.get("request_path")
                or "unknown"
            )
            classification = str(payload.get("classification") or "")
            detail_key = (venue, endpoint)
            detail = nonblocking_bulk_probe_details.setdefault(
                detail_key,
                {
                    "venue": venue,
                    "endpoint": endpoint,
                    "count": 0,
                    "timeout_count": 0,
                    "fallback_planned_count": 0,
                    "no_fallback_count": 0,
                    "last_ts_ms": 0,
                    "diagnostic_scope": str(payload.get("diagnostic_scope") or ""),
                },
            )
            detail["count"] += 1
            if classification == "timeout":
                detail["timeout_count"] += 1
            if payload.get("fallback_planned") is True:
                detail["fallback_planned_count"] += 1
            elif payload.get("fallback_planned") is False:
                detail["no_fallback_count"] += 1
            detail["last_ts_ms"] = max(int(detail.get("last_ts_ms", 0) or 0), ts_ms)
            timeout_ms = _int_or_none(payload.get("timeout_ms"))
            if timeout_ms is not None:
                detail["timeout_ms"] = timeout_ms
            nonblocking_bulk_probe_by_venue[venue] += 1

        for record in records:
            ts_ms = int(record.get("ts_ms", 0) or 0)
            if ts_ms < since_ms:
                continue
            kind = str(record.get("kind", "") or "")
            payload = _payload(record)
            event_counts[kind] += 1
            if w_first == 0 or ts_ms < w_first:
                w_first = ts_ms
            if ts_ms > w_last:
                w_last = ts_ms

            readiness = payload.get("readiness_evidence", {})
            if not isinstance(readiness, dict):
                readiness = {}
            provider = str(payload.get("provider") or readiness.get("provider") or "")
            _record_code_side_blocker(
                kind=kind,
                payload=payload,
                category_counts=code_side_category_counts,
                reason_counts=code_side_reason_counts,
                resolution_counts=code_side_resolution_counts,
                oi_evidence_health_summary=code_side_oi_evidence_health_summary,
                filtered_out_counts=code_side_filtered_counts,
                exclude_strategy=exclude_strategy,
                exclude_liquidity=exclude_liquidity,
            )
            if kind == "recovery.live_position_bulk_diagnostic_error":
                _record_nonblocking_bulk_probe(payload, ts_ms)

            if kind in {
                "runtime.entry_blocked_local_l2_selection",
                "runtime.entry_blocked_ws_bbo_selection",
                "runtime.entry_blocked_admission_selection",
            }:
                reason = str(payload.get("reason", "unknown") or "unknown")
                if kind == "runtime.entry_blocked_admission_selection" or _is_entry_admission_selection(reason, payload):
                    entry_admission_blocker_counts[reason] += 1
                elif (
                    kind == "runtime.entry_blocked_ws_bbo_selection"
                    or provider == "ws_bbo_quote_lease"
                    or reason.startswith("entry_ws_bbo_quote_lease_")
                ):
                    entry_ws_bbo_blocker_counts[reason] += 1
                else:
                    entry_l2_blocker_counts[reason] += 1
                _inc_symbol_pair(payload, top_pairs, top_symbols)
            elif kind == "runtime.entry_local_l2_readiness_diagnostics":
                saw_not_ready = False
                for item in payload.get("not_ready", []) or []:
                    if not isinstance(item, dict):
                        continue
                    saw_not_ready = True
                    reason = str(item.get("reason", "unknown") or "unknown")
                    entry_l2_not_ready_reason_counts[reason] += 1
                    _inc_symbol_pair(item, top_pairs, top_symbols)
                if not saw_not_ready:
                    totals = payload.get("entry_local_l2_primary_not_ready_reason_totals", {}) or {}
                    for reason, count in totals.items():
                        entry_l2_not_ready_reason_counts[str(reason)] += int(count or 0)
            elif kind == "scan.no_entry_diagnostics":
                totals = payload.get("entry_local_l2_primary_not_ready_reason_totals", {}) or {}
                for reason, count in totals.items():
                    entry_l2_not_ready_reason_counts[str(reason)] += int(count or 0)
                ws_bbo_totals = payload.get("entry_ws_bbo_blocker_counts", {}) or {}
                for reason, count in ws_bbo_totals.items():
                    entry_ws_bbo_blocker_counts[str(reason)] += int(count or 0)
                admission_totals = payload.get("entry_admission_blocker_counts", {}) or {}
                for reason, count in admission_totals.items():
                    entry_admission_blocker_counts[str(reason)] += int(count or 0)
                    _record_candidate_starvation(reason, count)
                for reason, count in (payload.get("top_quote_blocker_buckets", {}) or {}).items():
                    _record_candidate_starvation(reason, count)
                for reason, count in (payload.get("open_interest_blocker_counts", {}) or {}).items():
                    _record_candidate_starvation(reason, count)
                for reason, count in (payload.get("execution_liquidity_blocked_counts", {}) or {}).items():
                    _record_candidate_starvation(reason, count)
                for reason, count in (payload.get("snapshot_freshness_blocked_counts", {}) or {}).items():
                    _record_candidate_starvation(reason, count)
                for item in payload.get("entry_local_l2_primary_not_ready_detail_samples", []) or []:
                    if isinstance(item, dict):
                        _inc_symbol_pair(item, top_pairs, top_symbols)
            elif kind == "runtime.snapshot_degraded":
                domains = payload.get("stale_degraded_domains") or payload.get("degraded_domains") or []
                for domain in domains:
                    snapshot_degraded_counts[str(domain)] += 1
            elif kind == "runtime.snapshot_stale":
                domains = payload.get("stale_degraded_domains") or ["snapshot_stale"]
                for domain in domains:
                    snapshot_stale_counts[str(domain)] += 1

            # Pending/reconcile events — prefer reason over outcome for specificity
            if kind.startswith("pending_entry."):
                reason = str(payload.get("reason", "") or "")
                outcome = str(payload.get("outcome", "") or "")
                if reason:
                    pending_entry_counts[f"{kind}:{reason}"] += 1
                elif outcome:
                    pending_entry_counts[f"{kind}:{outcome}"] += 1
                else:
                    pending_entry_counts[kind] += 1

            if kind.startswith("order."):
                order_event_counts[kind] += 1
                reason = str(
                    payload.get("response_classification")
                    or payload.get("reason")
                    or payload.get("outcome")
                    or payload.get("error")
                    or ""
                )
                if reason and reason not in {"attempt", "ack_accepted", "filled"}:
                    exchange_error_counts[reason] += 1

            if kind in {
                "runtime.entry_quote_revalidate_failed",
                "runtime.entry_ws_bbo_top_candidate_rewarm_failed",
                "runtime.quote_stale",
                "runtime.order_quote_stale_skipped",
                "runtime.order_quote_stale_health_summary",
                "execution.entry_liquidity_blocked",
            }:
                reason = str(
                    payload.get("reason")
                    or payload.get("outcome")
                    or payload.get("eligibility_class")
                    or ""
                )
                if not reason and "quote_stale" in kind:
                    reason = "quote_stale"
                _record_candidate_starvation(reason, 1, payload)

            if kind == "passive_maintenance.cancel_try_window":
                try:
                    fill_ratio = float(payload.get("fill_ratio", 0) or 0)
                except (TypeError, ValueError):
                    fill_ratio = 0.0
                if fill_ratio <= 0:
                    incident_counts["passive_maker_zero_fill"] += 1
                    incident_conclusions["passive_maker_zero_fill"] = "v1_parity"
            elif kind == "entry.aborted" and "fail_closed" in str(payload.get("reason", "") or ""):
                incident_counts["abort_fail_closed"] += 1
                incident_conclusions["abort_fail_closed"] = "insufficient_evidence"
            elif kind == "recovery.live_position_probe_venue_cooldown" and str(payload.get("venue", "")).lower() == "okx" and payload.get("classification") == "rate_limited":
                incident_counts["okx_recovery_probe_rate_limited"] += 1
                incident_conclusions["okx_recovery_probe_rate_limited"] = "official_doc"
            elif kind == "recovery.live_position_probe_unsupported_symbols" and str(payload.get("venue", "")).lower() == "okx":
                count = _okx_instrument_missing_skipped_count(payload)
                if count:
                    incident_counts["okx_instrument_missing_skipped"] += count
                    incident_conclusions["okx_instrument_missing_skipped"] = "official_doc"
            elif kind == "okx_recovery_probe_noise":
                if payload.get("rate_limit_error"):
                    incident_counts["okx_recovery_probe_rate_limited"] += 1
                    incident_conclusions["okx_recovery_probe_rate_limited"] = "official_doc"
                count = _okx_instrument_missing_skipped_count(payload)
                if count:
                    incident_counts["okx_instrument_missing_skipped"] += count
                    incident_conclusions["okx_instrument_missing_skipped"] = "official_doc"
            elif kind in ("runtime.local_l2_sequence_gap_rebuild", "runtime.local_l2_snapshot_error"):
                if _has_official_sequence_evidence(payload):
                    incident_counts["local_l2_official_rebuild"] += 1
                    incident_conclusions["local_l2_official_rebuild"] = "official_doc"
                else:
                    incident_conclusions.setdefault("local_l2_official_rebuild", "insufficient_evidence")
            elif kind == "runtime.snapshot_fallback_last_good" and _snapshot_fallback_blocked(payload):
                incident_counts["snapshot_fallback_blocking"] += 1
                incident_conclusions["snapshot_fallback_blocking"] = _snapshot_fallback_conclusion(payload)
            elif kind == "entry.opened":
                incident_counts["entry_opened"] += 1
                incident_conclusions["entry_opened"] = "insufficient_evidence"
            elif kind == "runtime.position_opened":
                incident_counts["position_opened"] += 1
                incident_conclusions["position_opened"] = "insufficient_evidence"

        # Build a flat key-value map for blocker reasons vs just event kinds
        blocker_reason_counts: dict[str, int] = dict(sorted(entry_l2_blocker_counts.items()))
        for reason, count in entry_ws_bbo_blocker_counts.items():
            blocker_reason_counts[reason] = blocker_reason_counts.get(reason, 0) + count
        for reason, count in entry_admission_blocker_counts.items():
            blocker_reason_counts[reason] = blocker_reason_counts.get(reason, 0) + count
        for reason, count in entry_l2_not_ready_reason_counts.items():
            blocker_reason_counts[reason] = blocker_reason_counts.get(reason, 0) + count
        # Merge pending_entry reason-suffixed counts into classification
        for key, count in pending_entry_counts.items():
            blocker_reason_counts[key] = count

        nonblocking_bulk_probe_total = sum(nonblocking_bulk_probe_by_venue.values())
        nonblocking_bulk_probe_timeout_count = sum(
            int(detail.get("timeout_count", 0) or 0)
            for detail in nonblocking_bulk_probe_details.values()
        )
        nonblocking_bulk_probe_fallback_count = sum(
            int(detail.get("fallback_planned_count", 0) or 0)
            for detail in nonblocking_bulk_probe_details.values()
        )
        nonblocking_bulk_probe_no_fallback_count = sum(
            int(detail.get("no_fallback_count", 0) or 0)
            for detail in nonblocking_bulk_probe_details.values()
        )
        quote_stale_count = sum(
            count
            for reason, count in candidate_starvation_reasons.items()
            if "quote_stale" in reason
        )
        oi_structural_count = sum(
            count
            for reason, count in candidate_starvation_reasons.items()
            if "open_interest" in reason or "perp_oi" in reason or reason.startswith("oi_")
        )
        entry_opened_count = event_counts.get("entry.opened", 0) + event_counts.get("runtime.position_opened", 0)
        candidate_starvation_detected = (
            entry_opened_count == 0
            and (quote_stale_count > 0 or oi_structural_count > 0)
        )

        return {
            "event_counts": dict(sorted(event_counts.items())),
            "entry_l2_blocker_counts": dict(sorted(entry_l2_blocker_counts.items())),
            "entry_ws_bbo_blocker_counts": dict(sorted(entry_ws_bbo_blocker_counts.items())),
            "entry_admission_blocker_counts": dict(sorted(entry_admission_blocker_counts.items())),
            "top_pairs": [
                {"pair_id": pair_id, "count": count}
                for pair_id, count in top_pairs.most_common(20)
            ],
            "top_symbols": [
                {"symbol": symbol, "count": count}
                for symbol, count in top_symbols.most_common(20)
            ],
            "entry_l2_not_ready_reason_counts": dict(sorted(entry_l2_not_ready_reason_counts.items())),
            "snapshot_degraded_counts": dict(sorted(snapshot_degraded_counts.items())),
            "snapshot_stale_counts": dict(sorted(snapshot_stale_counts.items())),
            "order_event_counts": dict(sorted(order_event_counts.items())),
            "exchange_error_counts": dict(sorted(exchange_error_counts.items())),
            "pending_entry_counts": dict(sorted(pending_entry_counts.items())),
            "incident_counts": dict(sorted(incident_counts.items())),
            "incident_conclusions": dict(sorted(incident_conclusions.items())),
            "code_side_blocker_view": _code_side_view(
                category_counts=code_side_category_counts,
                reason_counts=code_side_reason_counts,
                resolution_counts=code_side_resolution_counts,
                oi_evidence_health_summary=code_side_oi_evidence_health_summary,
                filtered_out_counts=code_side_filtered_counts,
                exclude_strategy=exclude_strategy,
                exclude_liquidity=exclude_liquidity,
            ),
            "blocker_reason_counts": blocker_reason_counts,
            "nonblocking_bulk_probe_summary": {
                "total_count": nonblocking_bulk_probe_total,
                "timeout_count": nonblocking_bulk_probe_timeout_count,
                "by_venue": dict(sorted(nonblocking_bulk_probe_by_venue.items())),
                "fallback_planned_count": nonblocking_bulk_probe_fallback_count,
                "no_fallback_count": nonblocking_bulk_probe_no_fallback_count,
                "details": sorted(
                    nonblocking_bulk_probe_details.values(),
                    key=lambda item: (str(item.get("venue", "")), str(item.get("endpoint", ""))),
                ),
            },
            "candidate_selection_starvation": {
                "detected": candidate_starvation_detected,
                "total_blocker_count": sum(candidate_starvation_reasons.values()),
                "quote_stale_count": quote_stale_count,
                "open_interest_structural_count": oi_structural_count,
                "top_reasons": [
                    {"reason": reason, "count": count}
                    for reason, count in candidate_starvation_reasons.most_common(10)
                ],
                "top_symbols": [
                    {"symbol": symbol, "count": count}
                    for symbol, count in candidate_starvation_symbols.most_common(10)
                ],
                "action": "rewarm_top_candidates_and_reprobe_open_interest",
            },
            "first_ts_ms": w_first,
            "last_ts_ms": w_last,
        }

    result: dict[str, Any] = {
        "windows": {},
        "classification": {},
        "first_ts_ms": first_ts_ms,
        "last_ts_ms": last_ts_ms,
    }

    window_results: dict[str, dict[str, Any]] = {}
    for wname in windows:
        since = window_bounds.get(wname, 0)
        window_results[wname] = _analyze_records(all_records, since)

    result["windows"] = window_results

    # Build flat counts for classification
    counts_2h = window_results.get("last_2h", {}).get("blocker_reason_counts", {})
    counts_24h = window_results.get("last_24h", {}).get("blocker_reason_counts", {})
    counts_run = window_results.get("run_window", {}).get("blocker_reason_counts", {})
    result["classification"] = _classify_blockers(counts_2h, counts_24h, counts_run)

    return result


def _read_state_summary(state_path: Path) -> dict[str, Any]:
    """Read live-state.json and return a safe summary (no secrets)."""
    if not state_path.exists():
        return {"error": "state_file_not_found", "path": str(state_path)}
    try:
        state = json.loads(state_path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        return {"error": "state_file_unreadable", "detail": str(e)}
    summary: dict[str, Any] = {}
    # Safe fields: lifecycle, risk_mode, counts
    for key in ("lifecycle", "risk_mode", "open_positions", "pending_entries",
                "position_count", "pending_count", "snapshot_max_age_ms",
                "started_at_ms", "updated_at_ms"):
        val = state.get(key)
        if val is not None:
            summary[key] = val
    open_pos = state.get("open_positions", {})
    if isinstance(open_pos, dict):
        summary["open_position_count"] = len(open_pos)
    pending = state.get("pending_entries", {}) or state.get("pending_entrys", {})
    if isinstance(pending, dict):
        summary["pending_entry_count"] = len(pending)
        pending_summary = []
        for pid, pe in pending.items():
            if isinstance(pe, dict):
                pending_summary.append({
                    "entry_id": pid,
                    "symbol": pe.get("symbol", ""),
                    "maker_venue": str(pe.get("maker_venue", pe.get("long_venue", ""))),
                    "hedge_venue": str(pe.get("hedge_venue", pe.get("short_venue", ""))),
                    "maker_leg_filled": pe.get("maker_leg_filled", 0),
                    "hedge_leg_filled": pe.get("hedge_leg_filled", 0),
                    "has_hedge_inflight": bool(pe.get("hedge_inflight", "")),
                    "uncertain_outcome": pe.get("uncertain_outcome", False),
                    "repair_state": pe.get("repair_state", ""),
                })
        summary["pending_entry_details"] = pending_summary
    # Exclude: api_key, secret, credentials, account_id, wallet_address
    excluded_prefixes = ("api_", "secret_", "credential", "account_", "wallet_", "key_", "pass")
    for key in list(state.keys()):
        key_lower = key.lower()
        if any(key_lower.startswith(p) for p in excluded_prefixes):
            continue
    return summary


def _read_snapshot_summary(snapshot_path: Path) -> dict[str, Any]:
    """Read opportunity-input-snapshot.json and return a safe summary."""
    if not snapshot_path.exists():
        return {"error": "snapshot_file_not_found", "path": str(snapshot_path)}
    try:
        snap = json.loads(snapshot_path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        return {"error": "snapshot_file_unreadable", "detail": str(e)}
    summary: dict[str, Any] = {}
    if isinstance(snap, dict):
        venues = snap.get("venues") or snap.get("quote_venues") or []
        if isinstance(venues, list):
            summary["venue_count"] = len(venues)
            summary["venues"] = [str(v) for v in venues]
        elif isinstance(venues, dict):
            summary["venue_count"] = len(venues)
            summary["venues"] = list(venues.keys())
        candidates = snap.get("candidates") or snap.get("tradeable") or []
        if isinstance(candidates, list):
            summary["candidate_count"] = len(candidates)
        summary["snapshot_timestamp_ms"] = snap.get("snapshot_timestamp_ms") or snap.get("ts_ms", 0)
    return summary


def analyze(path: Path, since_ms: int = 0) -> dict[str, Any]:
    """Legacy API: single-window analysis (backward compatible)."""
    result = analyze_event_file(path, now_ms=0, windows=["run_window"])
    win = result["windows"].get("run_window", {})
    return {
        "event_counts": win.get("event_counts", {}),
        "entry_l2_blocker_counts": win.get("entry_l2_blocker_counts", {}),
        "entry_ws_bbo_blocker_counts": win.get("entry_ws_bbo_blocker_counts", {}),
        "entry_admission_blocker_counts": win.get("entry_admission_blocker_counts", {}),
        "top_pairs": win.get("top_pairs", []),
        "top_symbols": win.get("top_symbols", []),
        "entry_l2_not_ready_reason_counts": win.get("entry_l2_not_ready_reason_counts", {}),
        "snapshot_degraded_counts": win.get("snapshot_degraded_counts", {}),
        "snapshot_stale_counts": win.get("snapshot_stale_counts", {}),
        "order_event_counts": win.get("order_event_counts", {}),
        "exchange_error_counts": win.get("exchange_error_counts", {}),
        "first_ts_ms": result["first_ts_ms"],
        "last_ts_ms": result["last_ts_ms"],
    }


def _fmt_ts(ts_ms: int) -> str:
    if ts_ms <= 0:
        return "N/A"
    try:
        return datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OSError, ValueError):
        return str(ts_ms)


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze LightFee production blockers")
    parser.add_argument("journal", nargs="?", help="Journal JSONL path (legacy positional)")
    parser.add_argument("--json", dest="json_path", help="Journal JSONL path")
    parser.add_argument("--events", dest="events_path", help="Events JSONL path")
    parser.add_argument("--state", dest="state_path", help="Live state JSON path")
    parser.add_argument("--snapshot", dest="snapshot_path", help="Sidecar snapshot JSON path")
    parser.add_argument("--since-ms", type=int, default=0)
    parser.add_argument("--since", default="")
    parser.add_argument("--windows", default="last_2h,last_24h,run_window",
                        help="Comma-separated window names (default: last_2h,last_24h,run_window)")
    parser.add_argument("--no-secrets", action="store_true", default=True,
                        help="Strip secrets from output (default: on)")
    parser.add_argument("--exclude-strategy", action="store_true",
                        help="Add a code-side blocker view that filters strategy blockers")
    parser.add_argument("--exclude-liquidity", action="store_true",
                        help="Add a code-side blocker view that filters liquidity and OI blockers")
    args = parser.parse_args()

    events_path = args.events_path or args.json_path or args.journal
    if not events_path:
        parser.error("provide an events path with --events, --json, or positional journal")

    windows = [w.strip() for w in args.windows.split(",") if w.strip()]

    report = analyze_event_file(
        Path(events_path),
        windows=windows,
        exclude_strategy=args.exclude_strategy,
        exclude_liquidity=args.exclude_liquidity,
    )

    # Add state and snapshot summaries if provided
    if args.state_path:
        report["state_summary"] = _read_state_summary(Path(args.state_path))
    if args.snapshot_path:
        report["snapshot_summary"] = _read_snapshot_summary(Path(args.snapshot_path))

    # Add human-readable timestamps
    report["first_ts_human"] = _fmt_ts(report["first_ts_ms"])
    report["last_ts_human"] = _fmt_ts(report["last_ts_ms"])

    print(json.dumps(report, sort_keys=True, separators=(",", ":"), default=str))


if __name__ == "__main__":
    main()
