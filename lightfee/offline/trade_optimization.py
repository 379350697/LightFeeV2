"""Read-only trade sample analysis for strategy optimization audits."""

from __future__ import annotations

from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from typing import Any, Iterable, Iterator

from lightfee.lifecycle.exchange_truth_ledger import (
    LifecycleClassification,
    build_exchange_truth_lifecycle,
)


JsonDict = dict[str, Any]

MARKET_SNAPSHOT_KINDS = {
    "runtime.snapshot_freshness_decision",
    "runtime.entry_quote_evidence_resolved_by_ws_bbo",
}
COUNTERFACTUAL_KINDS = {"execution.entry_selected"}
DEFAULT_MARKET_MATCH_WINDOW_MS = 300_000


def iter_jsonl_events(paths: list[Path]) -> Iterator[JsonDict]:
    yield from _iter_jsonl_events_with_line_filter(paths)


def _iter_jsonl_events_with_line_filter(
    paths: list[Path],
    *,
    line_filter: Any | None = None,
    stats: JsonDict | None = None,
) -> Iterator[JsonDict]:
    for path in paths:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                if stats is not None:
                    stats["raw_line_count"] = int(stats.get("raw_line_count") or 0) + 1
                if line_filter is not None and not line_filter(line):
                    if stats is not None:
                        stats["line_filtered_count"] = int(stats.get("line_filtered_count") or 0) + 1
                    continue
                try:
                    record = json.loads(line)
                except Exception:
                    if stats is not None:
                        stats["json_error_count"] = int(stats.get("json_error_count") or 0) + 1
                    continue
                if isinstance(record, dict):
                    if stats is not None:
                        stats["parsed_event_count"] = int(stats.get("parsed_event_count") or 0) + 1
                    yield record


def read_jsonl_events(paths: list[Path]) -> list[JsonDict]:
    return list(iter_jsonl_events(paths))


def read_trade_optimization_events(
    paths: list[Path],
    *,
    include_counterfactual: bool = False,
    market_match_window_ms: int = DEFAULT_MARKET_MATCH_WINDOW_MS,
) -> tuple[list[JsonDict], JsonDict]:
    """Read only the event subset needed for historical trade optimization.

    Full production history can contain very large quote/snapshot noise. The
    optimizer only needs position-scoped lifecycle/accounting evidence plus
    market snapshots near the observed lifecycle timestamps.
    """

    selected_events: list[JsonDict] = []
    counterfactual_events: list[JsonDict] = []
    market_windows: dict[str, list[tuple[int, int, set[str]]]] = defaultdict(list)
    position_ids: set[str] = set()
    position_event_count = 0
    counterfactual_event_count = 0
    first_pass_stats: JsonDict = {}
    first_pass_filter = _line_contains_any(
        _first_pass_line_tokens(include_counterfactual=include_counterfactual)
    )

    for event in _iter_jsonl_events_with_line_filter(
        paths,
        line_filter=first_pass_filter,
        stats=first_pass_stats,
    ):
        kind = str(event.get("kind") or "")
        payload = _payload(event)
        if _is_trade_position_event(kind, payload):
            selected_events.append(event)
            position_event_count += 1
            position_id = _position_id(payload)
            if position_id:
                position_ids.add(position_id)
            _add_market_windows_from_event(
                market_windows,
                event,
                market_match_window_ms=market_match_window_ms,
            )
        elif include_counterfactual and kind in COUNTERFACTUAL_KINDS:
            counterfactual_events.append(event)
            counterfactual_event_count += 1

    raw_market_window_count = sum(len(windows) for windows in market_windows.values())
    merged_market_windows = _merge_market_windows(market_windows)

    market_events: list[JsonDict] = []
    market_pass_stats: JsonDict = {}
    if merged_market_windows:
        market_pass_filter = _line_contains_market_snapshot_for_symbols(
            set(merged_market_windows)
        )
        for event in _iter_jsonl_events_with_line_filter(
            paths,
            line_filter=market_pass_filter,
            stats=market_pass_stats,
        ):
            kind = str(event.get("kind") or "")
            if kind not in MARKET_SNAPSHOT_KINDS:
                continue
            if _market_event_in_windows(event, merged_market_windows):
                market_events.append(event)

    selected = sorted(
        selected_events + market_events + counterfactual_events,
        key=_event_ts_ms,
    )
    event_filter = {
        "enabled": True,
        "raw_event_count": int(first_pass_stats.get("raw_line_count") or 0),
        "selected_event_count": len(selected),
        "first_pass_parsed_event_count": int(first_pass_stats.get("parsed_event_count") or 0),
        "first_pass_line_filtered_count": int(first_pass_stats.get("line_filtered_count") or 0),
        "first_pass_json_error_count": int(first_pass_stats.get("json_error_count") or 0),
        "market_pass_parsed_event_count": int(market_pass_stats.get("parsed_event_count") or 0),
        "market_pass_line_filtered_count": int(market_pass_stats.get("line_filtered_count") or 0),
        "market_pass_json_error_count": int(market_pass_stats.get("json_error_count") or 0),
        "position_event_count": position_event_count,
        "market_event_count": len(market_events),
        "counterfactual_event_count": counterfactual_event_count,
        "position_count": len(position_ids),
        "raw_market_window_count": raw_market_window_count,
        "market_window_count": sum(len(windows) for windows in merged_market_windows.values()),
        "market_window_symbol_count": len(merged_market_windows),
    }
    return selected, event_filter


def build_trade_optimization_analysis(
    events: list[JsonDict],
    *,
    normal_only: bool = True,
    include_counterfactual: bool = False,
    market_match_window_ms: int = DEFAULT_MARKET_MATCH_WINDOW_MS,
) -> JsonDict:
    positions: dict[str, JsonDict] = {}
    market_events: list[JsonDict] = []
    counterfactual_events: list[JsonDict] = []
    event_counts: Counter[str] = Counter()
    lifecycle_report = build_exchange_truth_lifecycle(events)
    lifecycle_positions = lifecycle_report.get("positions", {})
    if not isinstance(lifecycle_positions, dict):
        lifecycle_positions = {}

    for event in sorted(events, key=_event_ts_ms):
        kind = str(event.get("kind") or "")
        event_counts[kind] += 1
        payload = _payload(event)
        ts_ms = _event_ts_ms(event)
        if kind in MARKET_SNAPSHOT_KINDS:
            market_events.append({"ts_ms": ts_ms, "kind": kind, "payload": payload})
        if include_counterfactual and kind in COUNTERFACTUAL_KINDS:
            counterfactual_events.append({"ts_ms": ts_ms, "kind": kind, "payload": payload})

        position_id = _position_id(payload)
        if not position_id:
            continue
        position = positions.setdefault(position_id, _new_position(position_id))
        if kind == "entry.opened":
            position["entry"] = payload
            position["entry_ts_ms"] = ts_ms or int(payload.get("opened_at_ms") or 0)
        elif kind == "exit.reconciled":
            position["exit_reconciled"] = {"ts_ms": ts_ms, "payload": payload}
        elif kind == "exit.closed":
            position["exit_closed"].append({"ts_ms": ts_ms, "payload": payload})
        elif kind == "order.filled":
            position["fills"].append({"ts_ms": ts_ms, "payload": payload})
        elif kind.startswith("exit_shadow."):
            position["exit_shadow"].append({"ts_ms": ts_ms, "kind": kind, "payload": payload})
        elif _is_execution_event_kind(kind):
            position["execution_events"].append({"ts_ms": ts_ms, "kind": kind, "payload": payload})

    samples: list[JsonDict] = []
    excluded: list[JsonDict] = []
    for position_id, position in sorted(positions.items()):
        entry = position.get("entry")
        if not isinstance(entry, dict):
            continue
        sample, reason = _build_position_sample(
            position_id,
            position,
            market_events=market_events,
            market_match_window_ms=market_match_window_ms,
            lifecycle_truth=(
                lifecycle_positions.get(position_id)
                if isinstance(lifecycle_positions.get(position_id), dict)
                else None
            ),
        )
        if sample is not None:
            samples.append(sample)
        elif not normal_only:
            excluded.append(_excluded_position(position_id, entry, reason or "not_normal"))
        else:
            excluded.append(_excluded_position(position_id, entry, reason or "not_normal"))

    aggregates = _build_aggregates(samples)
    counterfactual = (
        _build_counterfactual_summary(counterfactual_events)
        if include_counterfactual
        else {"selected_count": 0, "by_symbol": {}, "by_route": {}}
    )
    recommendations = _build_recommendations(samples, aggregates)

    return {
        "summary": {
            "event_count": sum(event_counts.values()),
            "event_kind_counts": dict(sorted(event_counts.items())),
            "entry_opened_positions": sum(
                1
                for position in positions.values()
                if isinstance(position.get("entry"), dict)
            ),
            "normal_sample_count": len(samples),
            "excluded_position_count": len(excluded),
            "coverage_gap_count": sum(len(sample["coverage_gaps"]) for sample in samples),
            "normal_only": normal_only,
            "include_counterfactual": include_counterfactual,
            "lifecycle_truth_summary": lifecycle_report.get("summary", {}),
        },
        "samples": samples,
        "excluded_positions": excluded,
        "aggregates": aggregates,
        "counterfactual": counterfactual,
        "recommendations": recommendations,
    }


def sample_rows_for_csv(report: JsonDict) -> list[JsonDict]:
    rows: list[JsonDict] = []
    for sample in report.get("samples") or []:
        if not isinstance(sample, dict):
            continue
        pnl = sample.get("pnl") if isinstance(sample.get("pnl"), dict) else {}
        features = sample.get("features") if isinstance(sample.get("features"), dict) else {}
        market = sample.get("market") if isinstance(sample.get("market"), dict) else {}
        execution = sample.get("execution") if isinstance(sample.get("execution"), dict) else {}
        entry_snapshot = (
            market.get("entry_snapshot")
            if isinstance(market.get("entry_snapshot"), dict)
            else {}
        )
        rows.append(
            {
                "position_id": sample.get("position_id"),
                "symbol": sample.get("symbol"),
                "route": sample.get("route"),
                "long_venue": sample.get("long_venue"),
                "short_venue": sample.get("short_venue"),
                "normality_source": sample.get("normality_source"),
                "verification_status": sample.get("verification_status"),
                "open_ts_ms": sample.get("open_ts_ms"),
                "close_ts_ms": sample.get("close_ts_ms"),
                "hold_duration_ms": features.get("hold_duration_ms"),
                "quantity": features.get("quantity"),
                "notional_quote": features.get("notional_quote"),
                "selected_edge_bps": features.get("selected_edge_bps"),
                "time_to_funding_ms": features.get("time_to_funding_ms"),
                "funding_capture_ratio": features.get("funding_capture_ratio"),
                "fee_drag_bps": features.get("fee_drag_bps"),
                "close_markout_bps": features.get("close_markout_bps"),
                "funding_pnl_bps": features.get("funding_pnl_bps"),
                "realized_edge_after_cost_bps": features.get(
                    "realized_edge_after_cost_bps"
                ),
                "price_pnl_quote": pnl.get("price_pnl_quote"),
                "funding_pnl_quote": pnl.get("funding_pnl_quote"),
                "entry_fee_quote": pnl.get("entry_fee_quote"),
                "exit_fee_quote": pnl.get("exit_fee_quote"),
                "rebate_adjustment_quote": pnl.get("rebate_adjustment_quote"),
                "net_pnl_quote": pnl.get("net_pnl_quote"),
                "net_pnl_bps": pnl.get("net_pnl_bps"),
                "close_path": execution.get("close_path"),
                "entry_spread_bps": features.get("entry_spread_bps")
                or entry_snapshot.get("spread_bps"),
                "entry_spread_bucket": features.get("entry_spread_bucket"),
                "passive_wait_cost_observed": execution.get(
                    "passive_wait_cost_observed"
                ),
                "coverage_gaps": ",".join(sample.get("coverage_gaps") or []),
            }
        )
    return rows


def render_markdown_report(report: JsonDict) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    aggregates = report.get("aggregates") if isinstance(report.get("aggregates"), dict) else {}
    lines = [
        "# Trade Optimization Sample Report",
        "",
        "## Summary",
        "",
        f"- Entry opened positions: {summary.get('entry_opened_positions', 0)}",
        f"- Normal samples: {summary.get('normal_sample_count', 0)}",
        f"- Excluded positions: {summary.get('excluded_position_count', 0)}",
        f"- Coverage gaps: {summary.get('coverage_gap_count', 0)}",
        "",
        "## By Symbol",
        "",
    ]
    lines.extend(_markdown_aggregate_table(aggregates.get("by_symbol") or {}))
    lines.extend(["", "## By Route", ""])
    lines.extend(_markdown_aggregate_table(aggregates.get("by_route") or {}))
    lines.extend(["", "## By Close Path", ""])
    lines.extend(_markdown_aggregate_table(aggregates.get("by_close_path") or {}))
    lines.extend(["", "## By Entry Spread Bucket", ""])
    lines.extend(_markdown_aggregate_table(aggregates.get("by_entry_spread_bucket") or {}))
    lines.extend(["", "## Recommendations", ""])
    recommendations = report.get("recommendations") or []
    if not recommendations:
        lines.append("- No optimization recommendation met the minimum evidence rules.")
    for item in recommendations:
        if not isinstance(item, dict):
            continue
        lines.append(
            "- {kind}: {title} | tier={tier} | action={action} | "
            "samples={sample_count} | net={net_pnl_quote} | confidence={confidence}".format(
                kind=item.get("kind", ""),
                title=item.get("title", ""),
                tier=item.get("evidence_tier", ""),
                action=item.get("action_mode", ""),
                sample_count=item.get("sample_count", 0),
                net_pnl_quote=item.get("net_pnl_quote", "0"),
                confidence=item.get("confidence", "low"),
            )
        )
    lines.append("")
    return "\n".join(lines)


def _new_position(position_id: str) -> JsonDict:
    return {
        "position_id": position_id,
        "entry": None,
        "entry_ts_ms": 0,
        "exit_reconciled": None,
        "exit_closed": [],
        "fills": [],
        "exit_shadow": [],
        "execution_events": [],
    }


def _build_position_sample(
    position_id: str,
    position: JsonDict,
    *,
    market_events: list[JsonDict],
    market_match_window_ms: int,
    lifecycle_truth: JsonDict | None,
) -> tuple[JsonDict | None, str | None]:
    entry = position["entry"]
    if lifecycle_truth is not None:
        classification = str(lifecycle_truth.get("classification") or "")
        project_status = str(lifecycle_truth.get("project_record_status") or "")
        if classification != LifecycleClassification.EXCHANGE_LIFECYCLE_COMPLETE.value:
            return None, project_status or classification or "lifecycle_truth_not_complete"
        truth_pnl = (
            lifecycle_truth.get("pnl")
            if isinstance(lifecycle_truth.get("pnl"), dict)
            else {}
        )
        if not truth_pnl.get("evidence_refs"):
            return None, "exchange_truth_missing_pnl_evidence_refs"
        exit_source = _normality_source_from_lifecycle_truth(position, project_status)
        verification_status = "verified_exchange_lifecycle"
        exit_event = _latest_exit_event(position)
        exit_payload = (
            exit_event.get("payload")
            if isinstance(exit_event, dict) and isinstance(exit_event.get("payload"), dict)
            else {}
        )
        pnl = _pnl_label_from_lifecycle_truth(truth_pnl)
    else:
        exit_source, exit_event, verification_status, reject_reason = _select_verified_exit(position)
        if exit_event is None:
            return None, reject_reason
        exit_payload = exit_event["payload"]
        pnl = None
    open_ts_ms = int(position.get("entry_ts_ms") or entry.get("opened_at_ms") or 0)
    close_ts_ms = int(
        (exit_event or {}).get("ts_ms")
        or exit_payload.get("closed_at_ms")
        or _truth_close_ts_ms(lifecycle_truth)
        or 0
    )
    long_venue = str(entry.get("long_venue") or entry.get("long_exchange") or "")
    short_venue = str(entry.get("short_venue") or entry.get("short_exchange") or "")
    quantity = _decimal(
        (lifecycle_truth or {}).get("target_quantity")
        or entry.get("quantity")
        or entry.get("matched_quantity")
    )
    truth_pnl = (
        lifecycle_truth.get("pnl")
        if isinstance(lifecycle_truth, dict) and isinstance(lifecycle_truth.get("pnl"), dict)
        else {}
    )
    notional = _decimal(truth_pnl.get("notional_quote")) or _notional_quote(entry, quantity)
    if pnl is None:
        pnl = _pnl_label(exit_payload, notional=notional)
    market, market_gaps = _market_context(
        symbol=str(entry.get("symbol") or exit_payload.get("symbol") or "").upper(),
        venues=[long_venue, short_venue],
        open_ts_ms=open_ts_ms,
        close_ts_ms=close_ts_ms,
        market_events=market_events,
        window_ms=market_match_window_ms,
    )
    execution = _execution_context(position, exit_source)
    shadow = _exit_shadow_summary(position.get("exit_shadow") or [])
    features = _features(entry, open_ts_ms, close_ts_ms, quantity, notional, pnl)
    entry_snapshot = market.get("entry_snapshot") if isinstance(market, dict) else {}
    entry_spread_bps = (
        entry_snapshot.get("spread_bps") if isinstance(entry_snapshot, dict) else None
    )
    features["entry_spread_bps"] = entry_spread_bps
    features["entry_spread_bucket"] = _entry_spread_bucket(entry_spread_bps)
    execution["passive_wait_cost_observed"] = bool(
        execution.get("passive_close_event_count")
        and _decimal(features.get("close_markout_bps")) < 0
    )
    coverage_gaps = list(market_gaps)
    if lifecycle_truth is not None:
        source_coverage = lifecycle_truth.get("source_coverage")
        if isinstance(source_coverage, dict):
            coverage_gaps.extend(
                str(gap) for gap in source_coverage.get("gaps", []) if str(gap)
            )
    if not pnl.get("evidence_refs"):
        coverage_gaps.append("missing_pnl_evidence_refs")
    if notional <= 0:
        coverage_gaps.append("missing_notional_quote")
    if features.get("time_to_funding_ms") is None:
        coverage_gaps.append("missing_time_to_funding")

    return (
        {
            "position_id": position_id,
            "symbol": str(entry.get("symbol") or exit_payload.get("symbol") or "").upper(),
            "long_venue": long_venue,
            "short_venue": short_venue,
            "route": f"{long_venue}->{short_venue}",
            "open_ts_ms": open_ts_ms,
            "close_ts_ms": close_ts_ms,
            "normality_source": exit_source,
            "verification_status": verification_status,
            "pnl": pnl,
            "features": features,
            "market": market,
            "execution": execution,
            "exit_shadow": shadow,
            "coverage_gaps": sorted(set(coverage_gaps)),
        },
        None,
    )


def _select_verified_exit(position: JsonDict) -> tuple[str, JsonDict | None, str, str | None]:
    reconciled = position.get("exit_reconciled")
    if isinstance(reconciled, dict):
        payload = reconciled.get("payload") if isinstance(reconciled.get("payload"), dict) else {}
        complete = str(payload.get("accounting_status") or "") == "complete"
        evidence_gap = bool(payload.get("evidence_gap"))
        pending_backfill = bool(payload.get("pending_backfill"))
        if complete and not evidence_gap and not pending_backfill:
            return (
                "exit.reconciled",
                reconciled,
                "verified_exchange_accounting",
                None,
            )
        return (
            "",
            None,
            "",
            "exit_reconciled_pending_backfill_or_evidence_gap",
        )

    legacy_events = position.get("exit_closed") or []
    for event in sorted(legacy_events, key=lambda item: int(item.get("ts_ms") or 0), reverse=True):
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        entry = position.get("entry") if isinstance(position.get("entry"), dict) else {}
        if not _legacy_exit_closed_full(entry, payload):
            continue
        has_fills, _, _ = _legacy_close_fill_evidence(entry, payload, position.get("fills") or [])
        if has_fills:
            return (
                "legacy.exit.closed",
                event,
                "verified_legacy_close_fills",
                None,
            )
        return (
            "",
            None,
            "",
            "legacy_exit_closed_missing_exchange_fill_evidence",
        )
    if legacy_events:
        return "", None, "", "legacy_exit_closed_incomplete_or_uncertain"
    return "", None, "", "missing_exit_reconciliation"


def _latest_exit_event(position: JsonDict) -> JsonDict | None:
    events: list[JsonDict] = []
    reconciled = position.get("exit_reconciled")
    if isinstance(reconciled, dict):
        events.append(reconciled)
    for event in position.get("exit_closed") or []:
        if isinstance(event, dict):
            events.append(event)
    if not events:
        return None
    return max(events, key=lambda item: int(item.get("ts_ms") or 0))


def _normality_source_from_lifecycle_truth(position: JsonDict, project_status: str) -> str:
    if isinstance(position.get("exit_reconciled"), dict) and project_status == "ok":
        return "exit.reconciled"
    if position.get("exit_closed") and project_status.startswith("legacy_exit_closed"):
        return "exchange.truth.legacy_project_gap"
    return "exchange.truth.lifecycle"


def _truth_close_ts_ms(lifecycle_truth: JsonDict | None) -> int:
    if not isinstance(lifecycle_truth, dict):
        return 0
    values = [
        _first_int(row.get("filled_at_ms"))
        for row in lifecycle_truth.get("close_legs", [])
        if isinstance(row, dict)
    ]
    return max(values) if values else 0


def _pnl_label_from_lifecycle_truth(payload: JsonDict) -> JsonDict:
    return {
        "price_pnl_quote": _decimal_str(_decimal(payload.get("price_pnl_quote"))),
        "funding_pnl_quote": _decimal_str(_decimal(payload.get("funding_pnl_quote"))),
        "entry_fee_quote": _decimal_str(_decimal(payload.get("entry_fee_quote"))),
        "exit_fee_quote": _decimal_str(_decimal(payload.get("exit_fee_quote"))),
        "rebate_adjustment_quote": _decimal_str(
            _decimal(payload.get("rebate_adjustment_quote"))
        ),
        "net_pnl_quote": _decimal_str(_decimal(payload.get("net_pnl_quote"))),
        "net_pnl_bps": _decimal_str(_decimal(payload.get("net_pnl_bps"))),
        "evidence_refs": list(payload.get("evidence_refs") or []),
    }


def _legacy_exit_closed_full(entry: JsonDict, payload: JsonDict) -> bool:
    quantity = _decimal(entry.get("quantity") or entry.get("matched_quantity"))
    if quantity <= 0:
        return False
    long_closed = _decimal(payload.get("long_closed_qty"))
    short_closed = _decimal(payload.get("short_closed_qty"))
    if bool(payload.get("long_uncertain")) or bool(payload.get("short_uncertain")):
        return False
    return long_closed >= quantity * Decimal("0.999") and short_closed >= quantity * Decimal("0.999")


def _legacy_close_fill_evidence(
    entry: JsonDict,
    payload: JsonDict,
    fills: list[JsonDict],
) -> tuple[bool, int, list[str]]:
    quantity = _decimal(entry.get("quantity") or entry.get("matched_quantity"))
    missing: list[str] = []
    evidence_count = 0
    for leg in ("long", "short"):
        expected_qty = _decimal(payload.get(f"{leg}_closed_qty")) or quantity
        ids = {
            str(payload.get(f"{leg}_order_id") or ""),
            str(payload.get(f"{leg}_client_order_id") or ""),
        }
        leg_qty = Decimal("0")
        for fill in fills:
            fill_payload = fill.get("payload") if isinstance(fill.get("payload"), dict) else {}
            identity = {
                str(fill_payload.get("order_id") or ""),
                str(fill_payload.get("client_order_id") or ""),
            }
            source = str(fill_payload.get("source") or "").lower()
            fill_leg = str(fill_payload.get("leg") or "").lower()
            matched_by_id = bool((ids - {""}) & (identity - {""}))
            matched_by_leg = "close" in source and fill_leg == leg
            if not matched_by_id and not matched_by_leg:
                continue
            leg_qty += _decimal(fill_payload.get("quantity") or fill_payload.get("filled_qty"))
            evidence_count += 1
        if expected_qty > 0 and leg_qty < expected_qty * Decimal("0.999"):
            missing.append(leg)
    return not missing, evidence_count, missing


def _pnl_label(payload: JsonDict, *, notional: Decimal) -> JsonDict:
    price = _decimal(payload.get("price_pnl"))
    funding = _decimal(payload.get("funding_pnl_quote") or payload.get("funding"))
    entry_fee = _fee_as_pnl(payload.get("entry_fee_quote"))
    exit_fee = _fee_as_pnl(payload.get("exit_fee_quote"))
    explicit_net = payload.get("net_quote")
    if explicit_net is None:
        explicit_net = payload.get("net_pnl_quote")
    computed = price + funding + entry_fee + exit_fee
    net = _decimal(explicit_net) if explicit_net is not None else computed
    adjustment = net - computed
    net_bps = Decimal("0")
    if notional > 0:
        net_bps = (net / notional) * Decimal("10000")
    return {
        "price_pnl_quote": _decimal_str(price),
        "funding_pnl_quote": _decimal_str(funding),
        "entry_fee_quote": _decimal_str(entry_fee),
        "exit_fee_quote": _decimal_str(exit_fee),
        "rebate_adjustment_quote": _decimal_str(adjustment),
        "net_pnl_quote": _decimal_str(net),
        "net_pnl_bps": _decimal_str(net_bps),
        "evidence_refs": _pnl_evidence_refs(payload),
    }


def _pnl_evidence_refs(payload: JsonDict) -> list[str]:
    refs: list[str] = []
    position_id = str(payload.get("position_id") or "")
    if position_id:
        refs.append(f"event:{position_id}")
    for key in (
        "long_order_id",
        "short_order_id",
        "long_client_order_id",
        "short_client_order_id",
        "order_id",
        "client_order_id",
    ):
        value = str(payload.get(key) or "")
        if value:
            refs.append(f"{key}:{value}")
    return refs


def _features(
    entry: JsonDict,
    open_ts_ms: int,
    close_ts_ms: int,
    quantity: Decimal,
    notional: Decimal,
    pnl: JsonDict,
) -> JsonDict:
    funding_ts = _first_int(
        entry.get("funding_ts"),
        entry.get("funding_timestamp_ms"),
        entry.get("next_funding_ts_ms"),
    )
    time_to_funding_ms: int | None = None
    if funding_ts and open_ts_ms:
        time_to_funding_ms = funding_ts - open_ts_ms
    selected_total_funding_edge_bps = _decimal(entry.get("selected_total_funding_edge_bps"))
    expected_funding = Decimal("0")
    if notional > 0 and selected_total_funding_edge_bps:
        expected_funding = notional * selected_total_funding_edge_bps / Decimal("10000")
    funding_capture_ratio: str | None = None
    if expected_funding:
        funding_capture_ratio = _decimal_str(
            _decimal(pnl.get("funding_pnl_quote")) / expected_funding
        )
    entry_fee_pnl = _decimal(pnl.get("entry_fee_quote"))
    exit_fee_pnl = _decimal(pnl.get("exit_fee_quote"))
    fee_drag_quote = -(entry_fee_pnl + exit_fee_pnl)
    if fee_drag_quote < 0:
        fee_drag_quote = Decimal("0")
    price_pnl_quote = _decimal(pnl.get("price_pnl_quote"))
    funding_pnl_quote = _decimal(pnl.get("funding_pnl_quote"))
    net_pnl_quote = _decimal(pnl.get("net_pnl_quote"))
    return {
        "quantity": _decimal_str(quantity),
        "notional_quote": _decimal_str(notional),
        "hold_duration_ms": close_ts_ms - open_ts_ms if open_ts_ms and close_ts_ms else None,
        "selected_edge_bps": _maybe_decimal_str(entry.get("selected_edge_bps")),
        "selected_total_funding_edge_bps": _maybe_decimal_str(
            entry.get("selected_total_funding_edge_bps")
        ),
        "time_to_funding_ms": time_to_funding_ms,
        "funding_capture_ratio": funding_capture_ratio,
        "fee_drag_bps": _decimal_str(_bps(fee_drag_quote, notional)),
        "close_markout_bps": _decimal_str(_bps(price_pnl_quote, notional)),
        "funding_pnl_bps": _decimal_str(_bps(funding_pnl_quote, notional)),
        "realized_edge_after_cost_bps": _decimal_str(_bps(net_pnl_quote, notional)),
    }


def _market_context(
    *,
    symbol: str,
    venues: list[str],
    open_ts_ms: int,
    close_ts_ms: int,
    market_events: list[JsonDict],
    window_ms: int,
) -> tuple[JsonDict, list[str]]:
    gaps: list[str] = []
    entry_snapshot = _nearest_market_snapshot(
        market_events,
        symbol=symbol,
        venues=venues,
        target_ts_ms=open_ts_ms,
        window_ms=window_ms,
    )
    exit_snapshot = _nearest_market_snapshot(
        market_events,
        symbol=symbol,
        venues=venues,
        target_ts_ms=close_ts_ms,
        window_ms=window_ms,
    )
    if entry_snapshot is None:
        gaps.append("missing_entry_market_snapshot")
    if exit_snapshot is None:
        gaps.append("missing_exit_market_snapshot")
    return {
        "entry_snapshot": entry_snapshot or {},
        "exit_snapshot": exit_snapshot or {},
    }, gaps


def _nearest_market_snapshot(
    market_events: list[JsonDict],
    *,
    symbol: str,
    venues: list[str],
    target_ts_ms: int,
    window_ms: int,
) -> JsonDict | None:
    if not target_ts_ms:
        return None
    venue_set = {venue.lower() for venue in venues if venue}
    candidates: list[tuple[int, JsonDict]] = []
    for event in market_events:
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        event_symbol = str(payload.get("symbol") or "").upper()
        event_venue = str(payload.get("venue") or payload.get("exchange") or "").lower()
        if symbol and event_symbol and event_symbol != symbol:
            continue
        if venue_set and event_venue and event_venue not in venue_set:
            continue
        age_ms = abs(int(event.get("ts_ms") or 0) - target_ts_ms)
        if age_ms > window_ms:
            continue
        candidates.append((age_ms, event))
    if not candidates:
        return None
    _, event = min(candidates, key=lambda item: item[0])
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    return _normalize_market_snapshot(event, payload, target_ts_ms)


def _normalize_market_snapshot(event: JsonDict, payload: JsonDict, target_ts_ms: int) -> JsonDict:
    bid = _first_decimal(
        payload.get("best_bid"),
        payload.get("bid"),
        payload.get("bid_price"),
        payload.get("best_bid_price"),
        payload.get("bbo_bid"),
        payload.get("quote_bid"),
    )
    ask = _first_decimal(
        payload.get("best_ask"),
        payload.get("ask"),
        payload.get("ask_price"),
        payload.get("best_ask_price"),
        payload.get("bbo_ask"),
        payload.get("quote_ask"),
    )
    spread_bps = Decimal("0")
    if bid > 0 and ask > 0:
        spread_bps = ((ask - bid) / ((ask + bid) / Decimal("2"))) * Decimal("10000")
    ts_ms = int(event.get("ts_ms") or 0)
    return {
        "ts_ms": ts_ms,
        "age_ms": abs(ts_ms - target_ts_ms),
        "source_kind": str(event.get("kind") or ""),
        "venue": str(payload.get("venue") or payload.get("exchange") or ""),
        "bid_price": _decimal_str(bid),
        "ask_price": _decimal_str(ask),
        "bid_size": _maybe_decimal_str(
            payload.get("bid_size")
            or payload.get("best_bid_size")
            or payload.get("bid_qty")
            or payload.get("quote_bid_size")
        ),
        "ask_size": _maybe_decimal_str(
            payload.get("ask_size")
            or payload.get("best_ask_size")
            or payload.get("ask_qty")
            or payload.get("quote_ask_size")
        ),
        "spread_bps": _decimal_str(spread_bps),
        "open_interest": _maybe_decimal_str(
            payload.get("open_interest")
            or payload.get("oi")
            or payload.get("open_interest_value")
            or payload.get("observed_open_interest_quote")
        ),
        "volume_24h": _maybe_decimal_str(
            payload.get("volume_24h")
            or payload.get("quote_volume")
            or payload.get("volume")
            or payload.get("observed_volume_24h_quote")
        ),
        "open_interest_evidence_status": str(
            payload.get("open_interest_evidence_status") or ""
        ),
        "freshness_status": str(
            payload.get("freshness_status")
            or payload.get("decision")
            or payload.get("status")
            or ""
        ),
    }


def _execution_context(position: JsonDict, exit_source: str) -> JsonDict:
    fills = position.get("fills") or []
    close_fill_count = 0
    fee_quote = Decimal("0")
    for fill in fills:
        payload = fill.get("payload") if isinstance(fill.get("payload"), dict) else {}
        fee_quote += _fee_as_pnl(payload.get("fee_quote"))
        phase = str(payload.get("phase") or "").lower()
        source = str(payload.get("source") or "").lower()
        trade_side = str(payload.get("tradeSide") or payload.get("trade_side") or "").lower()
        if phase == "close" or "close" in source or trade_side == "close":
            close_fill_count += 1
    execution_events = position.get("execution_events") or []
    event_kind_text = [
        str(event.get("kind") or "").lower()
        for event in execution_events
        if isinstance(event, dict)
    ]
    passive_count = sum(
        1 for kind in event_kind_text if kind.startswith("exit.passive")
    )
    reject_count = sum(
        1
        for kind in event_kind_text
        if "reject" in kind or "error" in kind
    )
    zero_fill_count = sum(
        1 for kind in event_kind_text if "zero_fill" in kind or "no_fill" in kind
    )
    reprice_count = sum(1 for kind in event_kind_text if "reprice" in kind)
    post_only_reject_count = sum(
        1
        for kind in event_kind_text
        if ("post_only" in kind or "post-only" in kind)
        and ("reject" in kind or "would_take" in kind or "blocked" in kind)
    )
    fallback_count = sum(
        1
        for kind in event_kind_text
        if "fallback" in kind or "dual_taker" in kind or "dual-taker" in kind
    )
    if exit_source == "legacy.exit.closed":
        close_path = "legacy"
    elif passive_count:
        close_path = "passive"
    else:
        close_path = "reconciled"
    return {
        "order_fill_count": len(fills),
        "close_fill_evidence_count": close_fill_count,
        "order_fill_fee_quote": _decimal_str(fee_quote),
        "passive_close_event_count": passive_count,
        "zero_fill_event_count": zero_fill_count,
        "reprice_event_count": reprice_count,
        "post_only_reject_event_count": post_only_reject_count,
        "fallback_event_count": fallback_count,
        "venue_error_or_reject_count": reject_count,
        "passive_wait_cost_observed": False,
        "close_path": close_path,
    }


def _exit_shadow_summary(events: list[JsonDict]) -> JsonDict:
    path_events = [
        event for event in events if str(event.get("kind") or "") == "exit_shadow.path_markout"
    ]
    if not path_events:
        return {"path_markout_count": 0, "strategy_summary_count": len(events)}
    net_values = [
        _decimal((event.get("payload") or {}).get("net_bps"))
        for event in path_events
        if isinstance(event.get("payload"), dict)
    ]
    adverse_values = [
        _decimal((event.get("payload") or {}).get("max_adverse_bps"))
        for event in path_events
        if isinstance(event.get("payload"), dict)
    ]
    return {
        "path_markout_count": len(path_events),
        "strategy_summary_count": sum(
            1 for event in events if str(event.get("kind") or "") == "exit_shadow.strategy_summary"
        ),
        "best_net_bps": _decimal_str(max(net_values) if net_values else Decimal("0")),
        "worst_net_bps": _decimal_str(min(net_values) if net_values else Decimal("0")),
        "max_adverse_bps": _decimal_str(max(adverse_values) if adverse_values else Decimal("0")),
    }


def _build_aggregates(samples: list[JsonDict]) -> JsonDict:
    return {
        "by_symbol": _aggregate_samples(samples, lambda sample: str(sample.get("symbol") or "")),
        "by_route": _aggregate_samples(samples, lambda sample: str(sample.get("route") or "")),
        "by_close_path": _aggregate_samples(
            samples,
            lambda sample: str((sample.get("execution") or {}).get("close_path") or ""),
        ),
        "by_time_to_funding_bucket": _aggregate_samples(
            samples,
            lambda sample: _time_to_funding_bucket(
                (sample.get("features") or {}).get("time_to_funding_ms")
            ),
        ),
        "by_entry_spread_bucket": _aggregate_samples(
            samples,
            lambda sample: str(
                (sample.get("features") or {}).get("entry_spread_bucket") or "unknown"
            ),
        ),
    }


def _aggregate_samples(samples: list[JsonDict], key_fn: Any) -> JsonDict:
    grouped: dict[str, JsonDict] = {}
    for sample in samples:
        key = key_fn(sample) or "UNKNOWN"
        row = grouped.setdefault(
            key,
            {
                "count": 0,
                "wins": 0,
                "net": Decimal("0"),
                "price": Decimal("0"),
                "funding": Decimal("0"),
                "fees": Decimal("0"),
                "notional": Decimal("0"),
                "net_bps": Decimal("0"),
                "fee_drag_bps": Decimal("0"),
                "close_markout_bps": Decimal("0"),
                "funding_capture_ratio": Decimal("0"),
                "funding_capture_ratio_count": 0,
                "coverage_gap_count": 0,
                "passive_wait_cost_count": 0,
            },
        )
        pnl = sample.get("pnl") if isinstance(sample.get("pnl"), dict) else {}
        features = sample.get("features") if isinstance(sample.get("features"), dict) else {}
        execution = sample.get("execution") if isinstance(sample.get("execution"), dict) else {}
        net = _decimal(pnl.get("net_pnl_quote"))
        row["count"] += 1
        row["wins"] += 1 if net > 0 else 0
        row["net"] += net
        row["price"] += _decimal(pnl.get("price_pnl_quote"))
        row["funding"] += _decimal(pnl.get("funding_pnl_quote"))
        row["fees"] += _decimal(pnl.get("entry_fee_quote")) + _decimal(pnl.get("exit_fee_quote"))
        row["notional"] += _decimal(features.get("notional_quote"))
        row["net_bps"] += _decimal(
            pnl.get("net_pnl_bps") or features.get("realized_edge_after_cost_bps")
        )
        row["fee_drag_bps"] += _decimal(features.get("fee_drag_bps"))
        row["close_markout_bps"] += _decimal(features.get("close_markout_bps"))
        if features.get("funding_capture_ratio") is not None:
            row["funding_capture_ratio"] += _decimal(features.get("funding_capture_ratio"))
            row["funding_capture_ratio_count"] += 1
        row["coverage_gap_count"] += len(sample.get("coverage_gaps") or [])
        row["passive_wait_cost_count"] += (
            1 if bool(execution.get("passive_wait_cost_observed")) else 0
        )
    finalized: JsonDict = {}
    for key, row in sorted(grouped.items()):
        count = int(row["count"])
        finalized[key] = {
            "count": count,
            "win_rate": _decimal_str(Decimal(row["wins"]) / Decimal(count) if count else 0),
            "net_pnl_quote": _decimal_str(row["net"]),
            "avg_net_pnl_quote": _decimal_str(row["net"] / Decimal(count) if count else 0),
            "price_pnl_quote": _decimal_str(row["price"]),
            "funding_pnl_quote": _decimal_str(row["funding"]),
            "fee_pnl_quote": _decimal_str(row["fees"]),
            "notional_quote": _decimal_str(row["notional"]),
            "avg_net_pnl_bps": _decimal_str(row["net_bps"] / Decimal(count) if count else 0),
            "avg_fee_drag_bps": _decimal_str(
                row["fee_drag_bps"] / Decimal(count) if count else 0
            ),
            "avg_close_markout_bps": _decimal_str(
                row["close_markout_bps"] / Decimal(count) if count else 0
            ),
            "avg_funding_capture_ratio": _decimal_str(
                row["funding_capture_ratio"] / Decimal(row["funding_capture_ratio_count"])
                if row["funding_capture_ratio_count"]
                else 0
            ),
            "coverage_gap_count": int(row["coverage_gap_count"]),
            "passive_wait_cost_count": int(row["passive_wait_cost_count"]),
        }
    return finalized


def _build_counterfactual_summary(events: list[JsonDict]) -> JsonDict:
    by_symbol: dict[str, JsonDict] = defaultdict(lambda: {"count": 0, "edge_bps_sum": Decimal("0")})
    by_route: dict[str, JsonDict] = defaultdict(lambda: {"count": 0, "edge_bps_sum": Decimal("0")})
    selected_count = 0
    for event in events:
        if str(event.get("kind") or "") != "execution.entry_selected":
            continue
        selected_count += 1
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        symbol = str(payload.get("symbol") or "UNKNOWN").upper()
        route = "{long}->{short}".format(
            long=str(payload.get("long_venue") or payload.get("long_exchange") or ""),
            short=str(payload.get("short_venue") or payload.get("short_exchange") or ""),
        )
        edge = _decimal(payload.get("selected_edge_bps"))
        by_symbol[symbol]["count"] += 1
        by_symbol[symbol]["edge_bps_sum"] += edge
        by_route[route]["count"] += 1
        by_route[route]["edge_bps_sum"] += edge
    return {
        "selected_count": selected_count,
        "by_symbol": _finalize_counterfactual_group(by_symbol),
        "by_route": _finalize_counterfactual_group(by_route),
    }


def _finalize_counterfactual_group(grouped: dict[str, JsonDict]) -> JsonDict:
    out: JsonDict = {}
    for key, row in sorted(grouped.items()):
        count = int(row["count"])
        out[key] = {
            "count": count,
            "avg_selected_edge_bps": _decimal_str(
                row["edge_bps_sum"] / Decimal(count) if count else 0
            ),
        }
    return out


def _build_recommendations(samples: list[JsonDict], aggregates: JsonDict) -> list[JsonDict]:
    recommendations: list[JsonDict] = []
    for group_name, kind, title_prefix in (
        ("by_symbol", "symbol_filter_review", "Review symbol filter"),
        ("by_route", "route_downweight_review", "Review venue route"),
        ("by_close_path", "close_path_review", "Review close path"),
        ("by_entry_spread_bucket", "entry_spread_bucket_review", "Review entry spread bucket"),
    ):
        group = aggregates.get(group_name) if isinstance(aggregates.get(group_name), dict) else {}
        for key, row in group.items():
            count = int(row.get("count") or 0)
            net = _decimal(row.get("net_pnl_quote"))
            if count < 2 or net >= 0:
                continue
            recommendations.append(
                _recommendation(
                    kind=kind,
                    title=f"{title_prefix}: {key}",
                    sample_count=count,
                    net_pnl_quote=net,
                    confidence=_confidence(count),
                    reason="Verified exchange-lifecycle samples in this bucket are net negative.",
                    estimated_impact_if_excluded_quote=_decimal_str(-net),
                    coverage_gap_count=int(row.get("coverage_gap_count") or 0),
                    avg_fee_drag_bps=row.get("avg_fee_drag_bps"),
                    avg_close_markout_bps=row.get("avg_close_markout_bps"),
                )
            )

    fee_drag_samples = [
        sample
        for sample in samples
        if _fee_drag_quote(sample)
        > abs(_decimal((sample.get("pnl") or {}).get("funding_pnl_quote")))
    ]
    if fee_drag_samples:
        recommendations.append(
            _recommendation(
                kind="entry_threshold_fee_drag_review",
                title="Review entry edge after real fee drag",
                sample_count=len(fee_drag_samples),
                net_pnl_quote=_sum_sample_net(fee_drag_samples),
                confidence=_confidence(len(fee_drag_samples)),
                reason="Real fee drag exceeded funding income on these verified samples.",
            )
        )
    passive_wait_samples = [
        sample
        for sample in samples
        if bool((sample.get("execution") or {}).get("passive_wait_cost_observed"))
    ]
    if passive_wait_samples:
        recommendations.append(
            _recommendation(
                kind="passive_close_wait_cost_observed",
                title="Review passive close wait cost",
                sample_count=len(passive_wait_samples),
                net_pnl_quote=_sum_sample_net(passive_wait_samples),
                confidence=_confidence(len(passive_wait_samples)),
                reason=(
                    "Passive close lifecycle events coincided with negative close markout; "
                    "keep as shadow evidence until sample size is stronger."
                ),
            )
        )
    coverage_gap_samples = [
        sample for sample in samples if sample.get("coverage_gaps")
    ]
    if coverage_gap_samples:
        recommendations.append(
            _recommendation(
                kind="market_data_coverage_gap",
                title="Keep market-feature conclusions out of live thresholds",
                sample_count=len(coverage_gap_samples),
                net_pnl_quote=_sum_sample_net(coverage_gap_samples),
                confidence="low",
                reason=(
                    "Some verified lifecycle samples lack entry/exit market snapshots or "
                    "source coverage, so market-feature conclusions are incomplete."
                ),
                evidence_tier="insufficient_evidence",
                coverage_gap_count=sum(
                    len(sample.get("coverage_gaps") or []) for sample in coverage_gap_samples
                ),
            )
        )
    if len(samples) < 30:
        recommendations.append(
            _recommendation(
                kind="sample_size_guardrail",
                title="Keep changes shadow-only until more verified samples accrue",
                sample_count=len(samples),
                net_pnl_quote=_sum_sample_net(samples),
                confidence="low",
                reason="Verified normal sample count is still small for live threshold changes.",
                evidence_tier="insufficient_evidence",
            )
        )
    return recommendations


def _recommendation(
    *,
    kind: str,
    title: str,
    sample_count: int,
    net_pnl_quote: Any,
    confidence: str,
    reason: str,
    evidence_tier: str = "shadow",
    **extra: Any,
) -> JsonDict:
    item = {
        "kind": kind,
        "title": title,
        "sample_count": sample_count,
        "net_pnl_quote": _decimal_str(net_pnl_quote),
        "confidence": confidence,
        "evidence_tier": evidence_tier,
        "action_mode": "observe_only",
        "blocks_live_threshold_change": False,
        "minimum_live_action_sample_count": 30,
        "reason": reason,
    }
    item.update(extra)
    return item


def _sum_sample_net(samples: list[JsonDict]) -> Decimal:
    return sum(
        (_decimal((sample.get("pnl") or {}).get("net_pnl_quote")) for sample in samples),
        Decimal("0"),
    )


def _fee_drag_quote(sample: JsonDict) -> Decimal:
    pnl = sample.get("pnl") if isinstance(sample.get("pnl"), dict) else {}
    fee_drag = -(_decimal(pnl.get("entry_fee_quote")) + _decimal(pnl.get("exit_fee_quote")))
    return fee_drag if fee_drag > 0 else Decimal("0")


def _excluded_position(position_id: str, entry: JsonDict, reason: str) -> JsonDict:
    return {
        "position_id": position_id,
        "symbol": str(entry.get("symbol") or "").upper(),
        "route": "{long}->{short}".format(
            long=str(entry.get("long_venue") or entry.get("long_exchange") or ""),
            short=str(entry.get("short_venue") or entry.get("short_exchange") or ""),
        ),
        "reason": reason,
    }


def _markdown_aggregate_table(group: JsonDict) -> list[str]:
    if not group:
        return ["No rows."]
    lines = ["| Bucket | Count | Net | Funding | Fees | Win Rate |", "|---|---:|---:|---:|---:|---:|"]
    for key, row in group.items():
        lines.append(
            f"| {key} | {row.get('count', 0)} | {row.get('net_pnl_quote', '0')} | "
            f"{row.get('funding_pnl_quote', '0')} | {row.get('fee_pnl_quote', '0')} | "
            f"{row.get('win_rate', '0')} |"
        )
    return lines


def _notional_quote(entry: JsonDict, quantity: Decimal) -> Decimal:
    prices = [
        _decimal(entry.get("long_entry_price")),
        _decimal(entry.get("short_entry_price")),
    ]
    prices = [price for price in prices if price > 0]
    if not prices or quantity <= 0:
        return Decimal("0")
    return quantity * (sum(prices, Decimal("0")) / Decimal(len(prices)))


def _time_to_funding_bucket(value: Any) -> str:
    if value is None:
        return "unknown"
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return "unknown"
    if ms < 0:
        return "past_funding_ts"
    if ms <= 5 * 60_000:
        return "0_5m"
    if ms <= 15 * 60_000:
        return "5_15m"
    if ms <= 60 * 60_000:
        return "15_60m"
    return "60m_plus"


def _entry_spread_bucket(value: Any) -> str:
    if value is None:
        return "missing"
    spread_bps = _decimal(value)
    if spread_bps <= 0:
        return "0bps"
    if spread_bps <= 5:
        return "0_5bps"
    if spread_bps <= 15:
        return "5_15bps"
    if spread_bps <= 50:
        return "15_50bps"
    return "50bps_plus"


def _bps(value: Decimal, notional: Decimal) -> Decimal:
    if notional <= 0:
        return Decimal("0")
    return (value / notional) * Decimal("10000")


def _confidence(count: int) -> str:
    if count >= 10:
        return "medium"
    return "low"


def _is_execution_event_kind(kind: str) -> bool:
    return (
        kind.startswith("exit.passive_close")
        or kind.startswith("runtime.normal_close")
        or kind.startswith("runtime.entry_post_only")
        or kind in {"order.rejected", "order.uncertain"}
    )


def _is_trade_position_event(kind: str, payload: JsonDict) -> bool:
    if _position_id(payload):
        return True
    if kind == "accounting.lifecycle_truth_rebuilt" and isinstance(payload.get("truth"), dict):
        return True
    return False


def _first_pass_line_tokens(*, include_counterfactual: bool) -> tuple[str, ...]:
    tokens = [
        '"entry.opened"',
        '"runtime.position_opened"',
        '"exit.reconciled"',
        '"exit.closed"',
        '"order.filled"',
        '"accounting.close_statement_backfill_corrected"',
        '"accounting.lifecycle_truth_rebuilt"',
        '"funding',
        '"exit_shadow.',
        '"exit.passive_close',
        '"runtime.normal_close',
        '"runtime.entry_post_only',
        '"order.rejected"',
        '"order.uncertain"',
    ]
    if include_counterfactual:
        tokens.append('"execution.entry_selected"')
    return tuple(tokens)


def _line_contains_any(tokens: tuple[str, ...]) -> Any:
    def _matches(line: str) -> bool:
        return any(token in line for token in tokens)

    return _matches


def _line_contains_market_snapshot_for_symbols(symbols: set[str]) -> Any:
    symbol_tokens = tuple(sorted(symbols))
    kind_tokens = tuple(f'"{kind}"' for kind in sorted(MARKET_SNAPSHOT_KINDS))

    def _matches(line: str) -> bool:
        return (
            any(kind in line for kind in kind_tokens)
            and any(symbol in line for symbol in symbol_tokens)
        )

    return _matches


def _add_market_windows_from_event(
    windows_by_symbol: dict[str, list[tuple[int, int, set[str]]]],
    event: JsonDict,
    *,
    market_match_window_ms: int,
) -> None:
    ts_ms = _event_ts_ms(event)
    if ts_ms <= 0:
        return
    payload = _payload(event)
    symbols = _event_symbols(payload)
    if not symbols:
        return
    venues = _event_venues(payload)
    start_ms = max(0, ts_ms - max(0, int(market_match_window_ms)))
    end_ms = ts_ms + max(0, int(market_match_window_ms))
    for symbol in symbols:
        windows_by_symbol[symbol].append((start_ms, end_ms, venues))


def _market_event_in_windows(
    event: JsonDict,
    windows_by_symbol: dict[str, list[tuple[int, int, set[str]]]],
) -> bool:
    ts_ms = _event_ts_ms(event)
    if ts_ms <= 0:
        return False
    payload = _payload(event)
    symbols = _event_symbols(payload)
    if not symbols:
        return False
    venues = _event_venues(payload)
    for symbol in symbols:
        for start_ms, end_ms, required_venues in windows_by_symbol.get(symbol, []):
            if ts_ms < start_ms or ts_ms > end_ms:
                continue
            if not required_venues or not venues or required_venues.intersection(venues):
                return True
    return False


def _merge_market_windows(
    windows_by_symbol: dict[str, list[tuple[int, int, set[str]]]],
) -> dict[str, list[tuple[int, int, set[str]]]]:
    merged_by_symbol: dict[str, list[tuple[int, int, set[str]]]] = {}
    for symbol, windows in windows_by_symbol.items():
        if not windows:
            continue
        merged_rows: list[list[Any]] = []
        for start_ms, end_ms, venues in sorted(windows, key=lambda row: (row[0], row[1])):
            venue_set = set(venues)
            if not merged_rows or start_ms > int(merged_rows[-1][1]):
                merged_rows.append([int(start_ms), int(end_ms), venue_set])
                continue
            merged_rows[-1][1] = max(int(merged_rows[-1][1]), int(end_ms))
            current_venues = merged_rows[-1][2]
            if not current_venues or not venue_set:
                merged_rows[-1][2] = set()
            else:
                current_venues.update(venue_set)
        merged_by_symbol[symbol] = [
            (int(start_ms), int(end_ms), set(venues))
            for start_ms, end_ms, venues in merged_rows
        ]
    return merged_by_symbol


def _event_symbols(payload: JsonDict) -> set[str]:
    values: list[Any] = [
        payload.get("symbol"),
        payload.get("instId"),
        payload.get("instrument_id"),
    ]
    truth = payload.get("truth")
    if isinstance(truth, dict):
        values.append(truth.get("symbol"))
    return {
        _canonical_symbol(value)
        for value in values
        if _canonical_symbol(value)
    }


def _event_venues(payload: JsonDict) -> set[str]:
    values: list[Any] = [
        payload.get("venue"),
        payload.get("exchange"),
        payload.get("long_venue"),
        payload.get("short_venue"),
        payload.get("long_exchange"),
        payload.get("short_exchange"),
        payload.get("maker_venue"),
        payload.get("hedge_venue"),
        payload.get("maker_exchange"),
        payload.get("hedge_exchange"),
    ]
    values.extend(_venue_values_from_rows(payload.get("long_legs")))
    values.extend(_venue_values_from_rows(payload.get("short_legs")))
    values.extend(_venue_values_from_rows(payload.get("statement_probe_candidates")))
    truth = payload.get("truth")
    if isinstance(truth, dict):
        values.extend(
            [
                truth.get("long_venue"),
                truth.get("short_venue"),
                truth.get("long_exchange"),
                truth.get("short_exchange"),
            ]
        )
        values.extend(_venue_values_from_rows(truth.get("open_legs")))
        values.extend(_venue_values_from_rows(truth.get("close_legs")))
    return {
        _canonical_venue(value)
        for value in values
        if _canonical_venue(value)
    }


def _venue_values_from_rows(rows: Any) -> Iterable[Any]:
    if not isinstance(rows, list):
        return []
    values: list[Any] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        values.extend([row.get("venue"), row.get("exchange")])
    return values


def _canonical_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _canonical_venue(value: Any) -> str:
    return str(value or "").strip().lower()


def _payload(event: JsonDict) -> JsonDict:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else {}


def _position_id(payload: JsonDict) -> str:
    return str(payload.get("position_id") or payload.get("entry_id") or "").strip()


def _event_ts_ms(event: JsonDict) -> int:
    return _first_int(event.get("ts_ms"), event.get("timestamp_ms"), event.get("time_ms")) or 0


def _first_int(*values: Any) -> int:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def _first_decimal(*values: Any) -> Decimal:
    for value in values:
        if value is None or value == "":
            continue
        parsed = _decimal(value)
        if parsed:
            return parsed
    return Decimal("0")


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _fee_as_pnl(value: Any) -> Decimal:
    fee = _decimal(value)
    if fee > 0:
        return -fee
    return fee


def _maybe_decimal_str(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return _decimal_str(_decimal(value))


def _decimal_str(value: Any) -> str:
    number = _decimal(value)
    if number == 0:
        return "0"
    text = format(number.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text == "-0" else text
