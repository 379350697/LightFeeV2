#!/usr/bin/env python3
"""Summarize production entry/local-L2 blockers from a JSONL journal.

The script is intentionally standalone: it reads only the supplied JSONL file
and emits a JSON report that can run on a cloud host in dry-run/read-only mode.
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


def analyze(path: Path, since_ms: int = 0) -> dict[str, Any]:
    event_counts: Counter[str] = Counter()
    entry_l2_blocker_counts: Counter[str] = Counter()
    top_pairs: Counter[str] = Counter()
    top_symbols: Counter[str] = Counter()
    entry_l2_not_ready_reason_counts: Counter[str] = Counter()
    snapshot_degraded_counts: Counter[str] = Counter()
    snapshot_stale_counts: Counter[str] = Counter()
    order_event_counts: Counter[str] = Counter()
    exchange_error_counts: Counter[str] = Counter()
    first_ts_ms = 0
    last_ts_ms = 0

    for record in _iter_records(path):
        ts_ms = int(record.get("ts_ms", 0) or 0)
        if ts_ms < since_ms:
            continue
        kind = str(record.get("kind", "") or "")
        payload = _payload(record)
        event_counts[kind] += 1
        if first_ts_ms == 0 or ts_ms < first_ts_ms:
            first_ts_ms = ts_ms
        if ts_ms > last_ts_ms:
            last_ts_ms = ts_ms

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
                for reason, count in (payload.get("reason_totals", {}) or {}).items():
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
            for symbol in payload.get("top_degraded_symbols", []) or []:
                top_symbols[str(symbol)] += 1
        elif kind == "runtime.snapshot_stale":
            domains = payload.get("stale_degraded_domains") or ["snapshot_stale"]
            for domain in domains:
                snapshot_stale_counts[str(domain)] += 1

        if kind.startswith("order."):
            order_event_counts[kind] += 1
            reason = str(
                payload.get("response_classification")
                or payload.get("reason")
                or payload.get("error")
                or ""
            )
            if reason and reason not in {"attempt", "ack_accepted", "filled"}:
                exchange_error_counts[reason] += 1

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
        "first_ts_ms": first_ts_ms,
        "last_ts_ms": last_ts_ms,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze LightFee production blockers")
    parser.add_argument("journal", nargs="?", help="Journal JSONL path")
    parser.add_argument("--json", dest="json_path", help="Journal JSONL path")
    parser.add_argument("--since-ms", type=int, default=0)
    parser.add_argument("--since", default="")
    args = parser.parse_args()

    journal_path = args.json_path or args.journal
    if not journal_path:
        parser.error("provide a journal path with --json or positional journal")
    since_ms = args.since_ms or _parse_since_ms(args.since)
    report = analyze(Path(journal_path), since_ms=since_ms)
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
