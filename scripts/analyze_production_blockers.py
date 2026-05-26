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


def _snapshot_fallback_blocked(payload: dict[str, Any]) -> bool:
    if payload.get("blocked") is True or payload.get("block_reason"):
        return True
    for item in payload.get("candidate_freshness_scope", []) or []:
        if isinstance(item, dict) and (
            item.get("blocked") is True or item.get("block_reason")
        ):
            return True
    return False


def _has_official_sequence_evidence(payload: dict[str, Any]) -> bool:
    if str(payload.get("venue", "")).lower() not in {"aster", "binance"}:
        return False
    return payload.get("previous_sequence_present") is True and all(
        payload.get(field) is not None
        for field in ("expected_previous_sequence", "raw_U", "raw_u", "raw_pu")
    )


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
        top_pairs: Counter[str] = Counter()
        top_symbols: Counter[str] = Counter()
        entry_l2_not_ready_reason_counts: Counter[str] = Counter()
        snapshot_degraded_counts: Counter[str] = Counter()
        snapshot_stale_counts: Counter[str] = Counter()
        order_event_counts: Counter[str] = Counter()
        exchange_error_counts: Counter[str] = Counter()
        pending_entry_counts: Counter[str] = Counter()
        incident_counts: Counter[str] = Counter()
        incident_conclusions: dict[str, str] = {}
        w_first = 0
        w_last = 0

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

            if kind == "runtime.entry_blocked_local_l2_selection":
                reason = str(payload.get("reason", "unknown") or "unknown")
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
                incident_conclusions["snapshot_fallback_blocking"] = (
                    "v1_parity" if payload.get("v1_parity_evidence") else "insufficient_evidence"
                )
            elif kind == "entry.opened":
                incident_counts["entry_opened"] += 1
                incident_conclusions["entry_opened"] = "insufficient_evidence"
            elif kind == "runtime.position_opened":
                incident_counts["position_opened"] += 1
                incident_conclusions["position_opened"] = "insufficient_evidence"

        # Build a flat key-value map for blocker reasons vs just event kinds
        blocker_reason_counts: dict[str, int] = dict(sorted(entry_l2_blocker_counts.items()))
        for reason, count in entry_l2_not_ready_reason_counts.items():
            blocker_reason_counts[reason] = blocker_reason_counts.get(reason, 0) + count
        # Merge pending_entry reason-suffixed counts into classification
        for key, count in pending_entry_counts.items():
            blocker_reason_counts[key] = count

        return {
            "event_counts": dict(sorted(event_counts.items())),
            "entry_l2_blocker_counts": dict(sorted(entry_l2_blocker_counts.items())),
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
            "blocker_reason_counts": blocker_reason_counts,
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
    args = parser.parse_args()

    events_path = args.events_path or args.json_path or args.journal
    if not events_path:
        parser.error("provide an events path with --events, --json, or positional journal")

    windows = [w.strip() for w in args.windows.split(",") if w.strip()]

    report = analyze_event_file(Path(events_path), windows=windows)

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
